-- Procedure for bulk upsert BOOKS
CREATE OR REPLACE PROCEDURE upsert_books_bulk(
    p_evt_ids IN SYS.ODCINUMBERLIST,
    p_bu_ids IN SYS.ODCINUMBERLIST,
    p_evt_status_ids IN SYS.ODCINUMBERLIST,
    p_veri_dates IN SYS.ODCIDATELIST,
    p_book_dates IN SYS.ODCIDATELIST,
    p_val_dates IN SYS.ODCIDATELIST,
    p_trx_dates IN SYS.ODCIDATELIST,
    p_perf_dates IN SYS.ODCIDATELIST
) AS
BEGIN
    FORALL i IN 1..p_evt_ids.COUNT
        MERGE INTO BOOKS b
        USING (SELECT p_evt_ids(i)        AS EVT_ID,
                      p_bu_ids(i)         AS BU_ID,
                      p_evt_status_ids(i) AS EVT_STATUS_ID,
                      p_veri_dates(i)     AS VERI_DATE,
                      p_book_dates(i)     AS BOOK_DATE,
                      p_val_dates(i)      AS VAL_DATE,
                      p_trx_dates(i)      AS TRX_DATE,
                      p_perf_dates(i)     AS PERF_DATE
               FROM DUAL) s
        ON (b.EVT_ID = s.EVT_ID)
        WHEN MATCHED THEN
            UPDATE
            SET BU_ID         = s.BU_ID,
                EVT_STATUS_ID = s.EVT_STATUS_ID,
                VERI_DATE     = s.VERI_DATE,
                BOOK_DATE     = s.BOOK_DATE,
                VAL_DATE      = s.VAL_DATE,
                TRX_DATE      = s.TRX_DATE,
                PERF_DATE     = s.PERF_DATE
        WHEN NOT MATCHED THEN
            INSERT (EVT_ID, BU_ID, EVT_STATUS_ID, VERI_DATE, BOOK_DATE, VAL_DATE, TRX_DATE, PERF_DATE)
            VALUES (s.EVT_ID, s.BU_ID, s.EVT_STATUS_ID, s.VERI_DATE, s.BOOK_DATE, s.VAL_DATE, s.TRX_DATE, s.PERF_DATE);

    COMMIT;
END;
/

-- Procedure for bulk upsert EVT_PKT
CREATE OR REPLACE PROCEDURE upsert_evt_pkt_bulk(
    p_evt_ids IN SYS.ODCINUMBERLIST,
    p_pkt_seq_nrs IN SYS.ODCINUMBERLIST,
    p_pos_ids IN SYS.ODCINUMBERLIST,
    p_qtys IN SYS.ODCINUMBERLIST,
    p_qty3s IN SYS.ODCINUMBERLIST,
    p_extl_book_texts IN SYS.ODCIVARCHAR2LIST
) AS
BEGIN
    FORALL i IN 1..p_evt_ids.COUNT
        MERGE INTO EVT_PKT e
        USING (SELECT p_evt_ids(i)         AS EVT_ID,
                      p_pkt_seq_nrs(i)     AS PKT_SEQ_NR,
                      p_pos_ids(i)         AS POS_ID,
                      p_qtys(i)            AS QTY,
                      p_qty3s(i)           AS QTY3,
                      p_extl_book_texts(i) AS EXTL_BOOK_TEXT
               FROM DUAL) s
        ON (e.EVT_ID = s.EVT_ID AND e.PKT_SEQ_NR = s.PKT_SEQ_NR)
        WHEN MATCHED THEN
            UPDATE
            SET POS_ID         = s.POS_ID,
                QTY            = s.QTY,
                QTY3           = s.QTY3,
                EXTL_BOOK_TEXT = s.EXTL_BOOK_TEXT
        WHEN NOT MATCHED THEN
            INSERT (EVT_ID, PKT_SEQ_NR, POS_ID, QTY, QTY3, EXTL_BOOK_TEXT)
            VALUES (s.EVT_ID, s.PKT_SEQ_NR, s.POS_ID, s.QTY, s.QTY3, s.EXTL_BOOK_TEXT);

    COMMIT;
END;
/

-- Procedure for bulk delete
CREATE OR REPLACE PROCEDURE delete_books_bulk(
    p_evt_ids IN SYS.ODCINUMBERLIST
) AS
BEGIN
    FORALL i IN 1..p_evt_ids.COUNT
        DELETE FROM BOOKS WHERE EVT_ID = p_evt_ids(i);

    COMMIT;
END;
/