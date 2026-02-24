-- создаём расширение (не обязательно, но полезно иметь под рукой)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1) главная таблица «заказы»
CREATE TABLE IF NOT EXISTS orders (
                                      order_uid        text PRIMARY KEY,      -- уникальный ID заказа (из JSON)
                                      track_number     text NOT NULL,         -- трек
                                      entry            text NOT NULL,         -- entry
                                      locale           text,                  -- en/ru и т.п.
                                      internal_signature text,                -- подпись
                                      customer_id      text,                  -- клиент
                                      delivery_service text,                  -- служба доставки
                                      shardkey         text,                  -- шардинг ключ
                                      sm_id            integer,               -- sm_id
                                      date_created     timestamptz NOT NULL,  -- дата/время создания (ISO8601 -> timestamptz)
                                      oof_shard        text                   -- ещё один шардинг ключ
);

-- ускорим поиск по треку (уникален в тестовой модели)
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_track ON orders(track_number);
CREATE INDEX IF NOT EXISTS idx_orders_date_created ON orders(date_created);

-- 2) доставка (1:1 c заказом)
CREATE TABLE IF NOT EXISTS deliveries (
                                          order_uid    text PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
    name         text,
    phone        text,
    zip          text,
    city         text,
    address      text,
    region       text,
    email        text
    );

-- 3) оплата (1:1 c заказом)
CREATE TABLE IF NOT EXISTS payments (
                                        order_uid      text PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
    transaction    text NOT NULL,
    request_id     text,
    currency       text,
    provider       text,
    amount         integer,
    payment_dt_raw bigint,         -- epoch, как пришёл
    payment_dt     timestamptz,    -- перевод в нормальную дату
    bank           text,
    delivery_cost  integer,
    goods_total    integer,
    custom_fee     integer
    );

-- 4) товары (1:many к заказу)
CREATE TABLE IF NOT EXISTS items (
                                     id            bigserial PRIMARY KEY,      -- внутренний ключ
                                     order_uid     text NOT NULL REFERENCES orders(order_uid) ON DELETE CASCADE,
    chrt_id       bigint,
    track_number  text,
    price         integer,
    rid           text,
    name          text,
    sale          integer,
    size          text,
    total_price   integer,
    nm_id         bigint,
    brand         text,
    status        integer
    );

CREATE INDEX IF NOT EXISTS idx_items_order_uid ON items(order_uid); -- частый джойн
