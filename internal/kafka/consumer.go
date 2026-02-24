package kafka

import (
	"OrderGo/internal/db/model"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"strings"
	"time"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})
}

func KafkaConsumer() {
	ctx := context.Background()

	// подключение к БД
	dsn := getenv("DATABASE_URL", "postgres://app:secret@localhost:5432/orders_db?sslmode=disable")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal("pg connect error:", err)
	}
	fmt.Println("dsn:", dsn)
	defer pool.Close()

	// подключение к Kafka
	kafkaURL := getenv("KAFKA_URL", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "topic")
	groupID := getenv("KAFKA_GROUP_ID", "groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID)
	defer reader.Close()

	fmt.Println("start consuming ...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatal("kafka read:", err)
		}

		var ord model.Order
		if err := json.Unmarshal(m.Value, &ord); err != nil {
			log.Printf("skip invalid json: %v payload=%s", err, string(m.Value))
			continue
		}

		// транзакция в БД
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Printf("tx begin: %v", err)
			continue
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// orders
		_, err = tx.Exec(ctx, `
			INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id,
			                    delivery_service, shardkey, sm_id, date_created, oof_shard)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (order_uid) DO UPDATE
			SET track_number=EXCLUDED.track_number,
			    entry=EXCLUDED.entry,
			    locale=EXCLUDED.locale,
			    internal_signature=EXCLUDED.internal_signature,
			    customer_id=EXCLUDED.customer_id,
			    delivery_service=EXCLUDED.delivery_service,
			    shardkey=EXCLUDED.shardkey,
			    sm_id=EXCLUDED.sm_id,
			    date_created=EXCLUDED.date_created,
			    oof_shard=EXCLUDED.oof_shard
		`, ord.OrderUID, ord.TrackNumber, ord.Entry, ord.Locale, ord.InternalSig, ord.CustomerID,
			ord.DeliveryService, ord.ShardKey, ord.SmID, ord.DateCreated, ord.OofShard)
		if err != nil {
			log.Printf("insert orders: %v", err)
			continue
		}

		// deliveries
		_, err = tx.Exec(ctx, `
			INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (order_uid) DO UPDATE
			SET name=$2, phone=$3, zip=$4, city=$5, address=$6, region=$7, email=$8
		`, ord.OrderUID, ord.Delivery.Name, ord.Delivery.Phone, ord.Delivery.Zip, ord.Delivery.City,
			ord.Delivery.Address, ord.Delivery.Region, ord.Delivery.Email)
		if err != nil {
			log.Printf("insert deliveries: %v", err)
			continue
		}

		// payments
		payTS := time.Unix(ord.Payment.PaymentDt, 0).UTC()
		_, err = tx.Exec(ctx, `
			INSERT INTO payments (order_uid, transaction, request_id, currency, provider, amount,
			                      payment_dt_raw, payment_dt, bank, delivery_cost, goods_total, custom_fee)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (order_uid) DO UPDATE
			SET transaction=$2, request_id=$3, currency=$4, provider=$5, amount=$6,
			    payment_dt_raw=$7, payment_dt=$8, bank=$9, delivery_cost=$10, goods_total=$11, custom_fee=$12
		`, ord.OrderUID, ord.Payment.Transaction, ord.Payment.RequestID, ord.Payment.Currency, ord.Payment.Provider,
			ord.Payment.Amount, ord.Payment.PaymentDt, payTS, ord.Payment.Bank, ord.Payment.DeliveryCost,
			ord.Payment.GoodsTotal, ord.Payment.CustomFee)
		if err != nil {
			log.Printf("insert payments: %v", err)
			continue
		}

		// items
		_, err = tx.Exec(ctx, `DELETE FROM items WHERE order_uid=$1`, ord.OrderUID)
		if err != nil {
			log.Printf("delete items: %v", err)
			continue
		}
		for _, it := range ord.Items {
			_, err = tx.Exec(ctx, `
				INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size,
				                   total_price, nm_id, brand, status)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			`, ord.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.RID, it.Name, it.Sale, it.Size,
				it.TotalPrice, it.NmID, it.Brand, it.Status)
			if err != nil {
				log.Printf("insert item: %v", err)
				break
			}
		}

		// commit
		if err == nil {
			if err = tx.Commit(ctx); err != nil {
				log.Printf("tx commit: %v", err)
				continue
			}
			fmt.Printf("saved order %s (partition=%d offset=%d)\n", ord.OrderUID, m.Partition, m.Offset)
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
