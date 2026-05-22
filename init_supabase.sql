-- Source table
CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    author_id INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Extraction and reconciliation indexes
CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON articles (updated_at);
CREATE INDEX IF NOT EXISTS idx_articles_created_at_id ON articles (created_at, id);
CREATE INDEX IF NOT EXISTS idx_articles_id ON articles (id);

-- Keep updated_at current on row updates
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_timestamp ON articles;
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON articles
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

-- Reset sample rows before seeding
TRUNCATE TABLE articles RESTART IDENTITY;

-- 2016 history
INSERT INTO articles (title, content, author_id, published_at, created_at, updated_at) VALUES
('Masa Depan Media Online 2016', 'Perkembangan internet di Indonesia sangat pesat...', 101, '2016-04-12 10:00:00+07', '2016-04-12 09:30:00+07', '2016-04-12 09:30:00+07'),
('Tips Menjadi Data Engineer', 'Langkah awal adalah menguasai SQL dan Python...', 102, '2016-08-25 15:45:00+07', '2016-08-25 15:00:00+07', '2016-08-25 15:00:00+07'),
('Kumparan Pertama Kali Diluncurkan', 'Hari ini merupakan tonggak sejarah bagi media baru...', 101, '2016-12-18 08:00:00+07', '2016-12-18 07:00:00+07', '2016-12-18 07:00:00+07');

-- 2020 history
INSERT INTO articles (title, content, author_id, published_at, created_at, updated_at) VALUES
('Tren Big Data di Tahun 2020', 'Teknologi cloud computing mendominasi industri...', 103, '2020-02-05 11:20:00+07', '2020-02-05 10:00:00+07', '2020-02-05 10:00:00+07'),
('Panduan Apache Airflow untuk Pemula', 'Orchestration menjadi kunci sukses data pipeline...', 102, '2020-11-30 14:00:00+07', '2020-11-30 13:00:00+07', '2020-11-30 13:00:00+07');

-- Recent rows
INSERT INTO articles (title, content, author_id, published_at, created_at, updated_at) VALUES
('Data Warehouse Modern dengan BigQuery', 'BigQuery sangat cocok untuk query analytics skala besar...', 103, NOW() - INTERVAL '12 hours', NOW() - INTERVAL '13 hours', NOW() - INTERVAL '13 hours'),
('Integrasi Supabase dan PostgreSQL', 'PostgreSQL di Supabase memudahkan real-time application...', 101, NOW() - INTERVAL '4 hours', NOW() - INTERVAL '5 hours', NOW() - INTERVAL '5 hours');

-- Soft-deleted row
INSERT INTO articles (title, content, author_id, published_at, created_at, updated_at, deleted_at) VALUES
('Artikel Terhapus Sementara', 'Konten ini sudah dihapus oleh editor secara soft-delete...', 104, NOW() - INTERVAL '1 day', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day');
