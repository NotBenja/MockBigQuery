#-- Opcion 2: denormalized: data_extractions_nested
CREATE TABLE `project.dataset.data_extractions_nested` (
    id STRING NOT NULL,
    filename STRING,
    title STRING,
    summary STRING,
    date DATE,
    -- ahora incluimos tag id para poder reconciliar con Firestore
    tags ARRAY < STRUCT < id INT64,
    -- tag id from Firestore
    name STRING,
    -- denormalized name copied from Firestore
    category STRING,
    -- denormalized category copied from Firestore
    version INT64 >>,
    pros ARRAY < STRING >,
    cons ARRAY < STRING >,
    authors ARRAY < STRING >,
    trade_ideas ARRAY < STRUCT < id STRING,
    recommendation STRING,
    summary STRING,
    conviction INT64,
    pros ARRAY < STRING >,
    cons ARRAY < STRING > >>,
    -- timestamp que indica cuándo se enriqueció/actualizó la info de tags
    tags_snapshot_ts TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) PARTITION BY DATE(date) CLUSTER BY id;