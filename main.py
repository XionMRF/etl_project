#!/bin/python

import pandas as pd
import jaydebeapi
import datetime
import glob
import os

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de2tm/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',
['de2tm', 'balinfundinson'],
'/home/de2tm/de/ojdbc8.jar')

curs = conn.cursor()

# Отключаем автокоммит
conn.jconn.setAutoCommit(False)

# Нахождение нужных файлов в папке
pass_black = glob.glob('/home/de2tm/RAMU/passport_blacklist*.xlsx')[0]
terminals = glob.glob('/home/de2tm/RAMU/terminals*.xlsx')[0]
transactions = glob.glob('/home/de2tm/RAMU/transactions*.csv')[0]

# Создание Df из плоских файлов
pass_blacklist_df = pd.read_excel(pass_black)
terminals_df = pd.read_excel(terminals)
transactions_df = pd.read_csv(transactions, sep=';', delimiter = ";")
transactions_df['amount'] = transactions_df['amount'].str.replace(',', '.')

# Извлекаем дату из terminals
terminals_dt = terminals.split('_')[1].split('.')[0]
terminals_dt = datetime.datetime.strptime(terminals_dt, '%d%m%Y')
terminals_dt = datetime.datetime.date(terminals_dt)
terminals_dt_1 = terminals_dt + datetime.timedelta(days=1)
terminals_dt_str = str(terminals_dt)
terminals_dt_1_str = str(terminals_dt_1)


# ---------Инкремент-----------

# Очистка стейджинга
curs.execute("DELETE FROM de2tm.ramu_stg_pssprt_blcklst")
curs.execute("DELETE FROM de2tm.ramu_stg_terminals")
curs.execute("DELETE FROM de2tm.ramu_stg_transactions")
curs.execute("DELETE FROM de2tm.ramu_stg_cards")
curs.execute("DELETE FROM de2tm.ramu_stg_accounts")
curs.execute("DELETE FROM de2tm.ramu_stg_clients")

curs.execute("DELETE FROM de2tm.ramu_stg_accounts_del")
curs.execute("DELETE FROM de2tm.ramu_stg_cards_del")
curs.execute("DELETE FROM de2tm.ramu_stg_clients_del")


# Загрузка в стейджинг

# Выборка новых записей из pass_blacklist_df
curs.execute("""SELECT last_update_dt FROM de2tm.ramu_meta_psb 
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_fact_pssprt_blcklst' """)

pass_meta_last_update_dt = curs.fetchone()[0]
pass_meta_last_update_dt = datetime.datetime.strptime(pass_meta_last_update_dt, '%Y-%m-%d %H:%M:%S')
pass_blacklist_df = pass_blacklist_df[pass_blacklist_df.date > pass_meta_last_update_dt]

# Преобразуем тип данных datetime столбца date в строку
pass_blacklist_df['date'] = pass_blacklist_df['date'].astype(str)


curs.executemany("""INSERT INTO de2tm.ramu_stg_pssprt_blcklst
                    VALUES (TO_DATE (?, 'YYYY-MM-DD'), ?)""", pass_blacklist_df.values.tolist())
   
   
curs.executemany("""INSERT INTO de2tm.ramu_stg_terminals
                    VALUES (?, ?, ?, ?)""", terminals_df.values.tolist())


curs.executemany("""INSERT INTO de2tm.ramu_stg_transactions
                    VALUES (?, ?, ?, ?, ?, ?, ?)""", transactions_df.values.tolist())


curs.execute("""INSERT INTO de2tm.ramu_stg_cards
            SELECT 
                card_num,
                account,
                create_dt,
                update_dt
            FROM bank.cards
            WHERE COALESCE(update_dt, create_dt) > (
                                                    SELECT last_update_dt
                                                    FROM de2tm.ramu_meta_psb 
                                                    WHERE SCHEMA = 'de2tm'
                                                    AND table_name = 'ramu_dwh_dim_cards_hist')""")


curs.execute("""INSERT INTO de2tm.ramu_stg_accounts
            SELECT 
                account,
                valid_to,
                client,
                create_dt,
                update_dt
            FROM bank.accounts
            WHERE COALESCE(update_dt, create_dt) > (
                                                    SELECT last_update_dt
                                                    FROM de2tm.ramu_meta_psb 
                                                    WHERE SCHEMA = 'de2tm'
                                                    AND table_name = 'ramu_dwh_dim_accounts_hist')""")


curs.execute("""INSERT INTO de2tm.ramu_stg_clients
            SELECT 
                client_id,
                last_name,
                first_name,
                patronymic,
                date_of_birth,
                passport_num,
                passport_valid_to,
                phone,
                create_dt,
                update_dt
            FROM bank.clients
            WHERE COALESCE(update_dt, create_dt) > (
                                                    SELECT last_update_dt
                                                    FROM de2tm.ramu_meta_psb 
                                                    WHERE SCHEMA = 'de2tm'
                                                    AND table_name = 'ramu_dwh_dim_clients_hist')""")


# Стейджинг для удаленний

curs.execute("""INSERT INTO de2tm.ramu_stg_accounts_del
                SELECT account
                FROM bank.accounts""")


curs.execute("""INSERT INTO de2tm.ramu_stg_cards_del
                SELECT card_num
                FROM bank.cards""")
    
    
curs.execute("""INSERT INTO de2tm.ramu_stg_clients_del
                SELECT client_id
                FROM bank.clients""")
                

# Заливка их в хранилище данных


curs.execute("""INSERT INTO de2tm.ramu_dwh_fact_pssprt_blcklst 
                (passport_num, entry_dt)
                    SELECT 
                        passport_num,
                        entry_dt
                    FROM de2tm.ramu_stg_pssprt_blcklst""")
                    
                    
curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_terminals_hist
                (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)	
                    SELECT 
                        stg.terminal_id,
                        stg.terminal_type,
                        stg.terminal_city,
                        stg.terminal_address,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_terminals stg
                    LEFT JOIN de2tm.ramu_dwh_dim_terminals_hist dwh
                    ON stg.terminal_id = dwh.terminal_id
                    WHERE dwh.terminal_id IS NULL""", (terminals_dt_1_str,))

curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_terminals_hist
                (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)	
                    SELECT 
                        stg.terminal_id,
                        stg.terminal_type,
                        stg.terminal_city,
                        stg.terminal_address,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_terminals stg
                    LEFT JOIN de2tm.ramu_dwh_dim_terminals_hist dwh
                        ON stg.terminal_id = dwh.terminal_id
                    WHERE stg.terminal_address != dwh.terminal_address
                    AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""", (terminals_dt_1_str,))
                    
curs.execute("""MERGE INTO de2tm.ramu_dwh_dim_terminals_hist dwh
                USING de2tm.ramu_stg_terminals stg
                ON (dwh.terminal_id = stg.terminal_id AND dwh.effective_from < TO_DATE(?, 'YYYY-MM-DD'))
                WHEN MATCHED THEN UPDATE SET 
                    dwh.effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""", (terminals_dt_1_str, terminals_dt_str,))

    
 
curs.execute("""INSERT INTO de2tm.ramu_dwh_fact_transactions
                (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
                SELECT 
                    CAST(transaction_id AS INT),
                    TO_TIMESTAMP(transaction_date, 'YYYY-MM-DD HH24:MI:SS'),
                    card_num,
                    oper_type,
                    CAST(amount AS DECIMAL(10,2)),
                    oper_result,
                    terminal
                FROM de2tm.ramu_stg_transactions""")


curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_cards_hist
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                    SELECT
                        card_num,
                        account,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_cards
                    WHERE update_dt IS NULL""")

curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_cards_hist
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                    SELECT
                        card_num,
                        account,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_cards
                    WHERE update_dt IS NOT NULL""")

curs.execute("""MERGE INTO de2tm.ramu_dwh_dim_cards_hist dwh
                USING de2tm.ramu_stg_cards stg
                ON (dwh.card_num = stg.card_num AND dwh.effective_from < COALESCE(stg.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    dwh.effective_to = stg.update_dt - 1
                WHERE dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")


curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_accounts_hist 
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                    SELECT
                        account_num,
                        valid_to,
                        client,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_accounts
                    WHERE update_dt IS NULL""")

curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_accounts_hist
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                    SELECT
                        account_num,
                        valid_to,
                        client,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_accounts
                    WHERE update_dt IS NOT NULL""")

curs.execute("""MERGE INTO de2tm.ramu_dwh_dim_accounts_hist dwh
                USING de2tm.ramu_stg_accounts stg
                ON (dwh.account_num = stg.account_num AND dwh.effective_from < COALESCE (stg.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    dwh.effective_to = stg.update_dt - 1
                WHERE dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")


curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_clients_hist
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_clients
                    WHERE update_dt IS NULL""")

curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_clients_hist
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM de2tm.ramu_stg_clients
                    WHERE update_dt IS NOT NULL""")

curs.execute("""MERGE INTO de2tm.ramu_dwh_dim_clients_hist dwh
                USING de2tm.ramu_stg_clients stg
                ON (dwh.client_id = stg.client_id AND dwh.effective_from < COALESCE(stg.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    dwh.effective_to = stg.update_dt - 1
                        WHERE dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")


# Установка флага на удаленние записей


curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_accounts_hist
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                SELECT
                    dwh.account_num,
                    dwh.valid_to,
                    dwh.client,
                    TO_DATE(?, 'YYYY-MM-DD'),
                    TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                    'Y'
                FROM de2tm.ramu_dwh_dim_accounts_hist dwh
                LEFT JOIN de2tm.ramu_stg_accounts_del del
                ON dwh.account_num = del.account_num
                WHERE del.account_num IS NULL
                AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND dwh.deleted_flg = 'N' """, (terminals_dt_str,))

curs.execute("""UPDATE de2tm.ramu_dwh_dim_accounts_hist
                SET effective_to = TO_DATE (?, 'YYYY-MM-DD')
                WHERE account_num IN (
                                      SELECT dwh.account_num
                                      FROM de2tm.ramu_dwh_dim_accounts_hist dwh
                                      LEFT JOIN de2tm.ramu_stg_accounts_del del
                                      ON dwh.account_num = del.account_num
                                      WHERE del.account_num IS NULL
                                      AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                      AND dwh.deleted_flg = 'N'
                                     )
                AND effective_to = TO_DATE ('5999-12-31', 'YYYY-MM-DD')
                AND deleted_flg = 'N' """, (terminals_dt_1_str,))

 
curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_cards_hist
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                SELECT
                    dwh.card_num,
                    dwh.account_num,
                    TO_DATE(?, 'YYYY-MM-DD'),
                    TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                    'Y'
                FROM de2tm.ramu_dwh_dim_cards_hist dwh
                LEFT JOIN de2tm.ramu_stg_cards_del del
                ON dwh.card_num = del.card_num
                WHERE del.card_num IS NULL
                AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND dwh.deleted_flg = 'N' """, (terminals_dt_str,))
    
curs.execute("""UPDATE de2tm.ramu_dwh_dim_cards_hist
                SET effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE card_num IN (
                                   SELECT dwh.card_num
                                   FROM de2tm.ramu_dwh_dim_cards_hist dwh
                                   LEFT JOIN de2tm.ramu_stg_cards_del del
                                   ON dwh.card_num = del.card_num
                                   WHERE del.card_num IS NULL
                                   AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                   AND dwh.deleted_flg = 'N'
                                  )
                AND effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND deleted_flg = 'N' """, (terminals_dt_1_str,))


curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_clients_hist
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                SELECT
                    dwh.client_id,
                    dwh.last_name,
                    dwh.first_name,
                    dwh.patronymic,
                    dwh.date_of_birth,
                    dwh.passport_num,
                    dwh.passport_valid_to,
                    dwh.phone,
                    TO_DATE(?, 'YYYY-MM-DD'),
                    TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                    'Y'
                FROM de2tm.ramu_dwh_dim_clients_hist dwh
                LEFT JOIN de2tm.ramu_stg_clients_del del
                ON dwh.client_id = del.client_id
                WHERE del.client_id IS NULL
                AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND dwh.deleted_flg = 'N' """, (terminals_dt_str,))
    
curs.execute("""UPDATE de2tm.ramu_dwh_dim_clients_hist
                SET effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE client_id IN (
                                   SELECT dwh.client_id
                                   FROM de2tm.ramu_dwh_dim_clients_hist dwh
                                   LEFT JOIN de2tm.ramu_stg_clients_del del
                                   ON dwh.client_id = del.client_id
                                   WHERE del.client_id IS NULL
                                   AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                   AND dwh.deleted_flg = 'N'
                                   )
                AND effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND deleted_flg = 'N' """, (terminals_dt_1_str,))
 

curs.execute("""INSERT INTO de2tm.ramu_dwh_dim_terminals_hist
                (terminal_id,	terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
                SELECT
                    dwh.terminal_id,
                    dwh.terminal_type,
                    dwh.terminal_city,
                    dwh.terminal_address,
                    TO_DATE(?, 'YYYY-MM-DD'),
                    TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                    'Y'
                FROM de2tm.ramu_dwh_dim_terminals_hist dwh
                LEFT JOIN de2tm.ramu_stg_terminals stg
                ON dwh.terminal_id = stg.terminal_id
                WHERE stg.terminal_id IS NULL
                AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND dwh.deleted_flg = 'N' """, (terminals_dt_str,))
    
curs.execute("""UPDATE de2tm.ramu_dwh_dim_terminals_hist
                SET effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE terminal_id IN (
                                     SELECT dwh.terminal_id
                                     FROM de2tm.ramu_dwh_dim_terminals_hist dwh
                                     LEFT JOIN de2tm.ramu_stg_terminals stg
                                     ON dwh.terminal_id = stg.terminal_id
                                     WHERE stg.terminal_id IS NULL
                                     AND dwh.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                     AND dwh.deleted_flg = 'N'
                                     )
                AND effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND deleted_flg = 'N' """, (terminals_dt_1_str,))



# Обновление метаданных

curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = (SELECT MAX (entry_dt) FROM de2tm.ramu_stg_pssprt_blcklst)
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_fact_pssprt_blcklst'""")
    
curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = TO_DATE (?, 'YYYY-MM-DD')
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_dim_terminals_hist' """, (terminals_dt_1_str,))

curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = (SELECT MAX (TO_DATE (transaction_date, 'YYYY-MM-DD HH24:MI:SS')) FROM de2tm.ramu_stg_transactions)
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_fact_transactions'""")
    
curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM de2tm.ramu_stg_cards)
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_dim_cards_hist'
                AND (SELECT MAX (COALESCE(update_dt, create_dt)) FROM ramu_stg_cards) IS NOT NULL""")
                
curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = (SELECT MAX (COALESCE (update_dt, create_dt)) FROM de2tm.ramu_stg_accounts)
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_dim_accounts_hist'
                AND (SELECT MAX (COALESCE(update_dt, create_dt)) FROM ramu_stg_accounts) IS NOT NULL""")

curs.execute("""UPDATE ramu_meta_psb  
                SET last_update_dt = (SELECT MAX (COALESCE (update_dt, create_dt)) FROM de2tm.ramu_stg_clients)
                WHERE SCHEMA = 'de2tm' AND table_name = 'ramu_dwh_dim_clients_hist'
                AND (SELECT MAX (COALESCE(update_dt, create_dt)) FROM ramu_stg_clients) IS NOT NULL""")


# Построение отчета

curs.execute("""INSERT INTO de2tm.ramu_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)	
                SELECT
                    event_dt,
                    passport,
                    fio,
                    phone,
                    event_type,
                    report_dt
                FROM (
                      SELECT
                            trans.trans_date AS event_dt,
                            clnts.passport_num AS passport,
                            clnts.last_name || ' ' || clnts.first_name || ' ' || clnts.patronymic AS fio,
                            clnts.phone AS phone,
                            CASE 
                            WHEN clnts.passport_valid_to IS NOT NULL
                            AND clnts.passport_num IN (SELECT passport_num FROM de2tm.ramu_dwh_fact_pssprt_blcklst)
                            OR clnts.passport_valid_to < TO_DATE (?, 'YYYY-MM-DD')
                            THEN 'Совершение операции при просроченном или заблокированном паспорте'
                            WHEN accnt.valid_to < TO_DATE (?, 'YYYY-MM-DD')
                            THEN 'Совершение операции при недействующем договоре'
                            WHEN trans.trans_date IN (
                                                     SELECT
                                                        MAX (trans1.trans_date)
                                                     FROM ramu_dwh_fact_transactions trans1
                                                     INNER JOIN ramu_dwh_dim_terminals_hist dwh_term_a
                                                        ON trans1.terminal = dwh_term_a.terminal_id
                                                     INNER JOIN ramu_dwh_fact_transactions trans2
                                                        ON trans1.card_num = trans2.card_num
                                                        AND trans1.trans_date < trans2.trans_date
                                                     INNER JOIN ramu_dwh_dim_terminals_hist dwh_term_b
                                                        ON trans2.terminal = dwh_term_b.terminal_id
                                                     WHERE trans1.card_num = trans2.card_num
                                                     AND dwh_term_a.terminal_city != dwh_term_b.terminal_city
                                                     AND (trans2.trans_date - trans1.trans_date) < INTERVAL '1' HOUR
                                                     )
                            THEN 'Совершение операции в разных городах в течение одного часа'
                            WHEN trans.trans_date IN (
                                                     SELECT 
                                                        MAX (trans1.trans_date)
                                                     FROM ramu_dwh_fact_transactions trans1
                                                     INNER JOIN ramu_dwh_fact_transactions trans2
                                                         ON trans1.card_num = trans2.card_num
                                                     AND trans1.trans_date < trans2.trans_date
                                                     INNER JOIN ramu_dwh_fact_transactions trans3
                                                         ON trans2.card_num = trans3.card_num
                                                     AND trans2.trans_date < trans3.trans_date
                                                     WHERE trans1.oper_result = 'REJECT' 
                                                     AND trans2.oper_result = 'REJECT' 
                                                     AND trans3.oper_result = 'SUCCESS'
                                                     AND trans1.amt > trans2.amt AND trans2.amt > trans3.amt
                                                     AND (trans3.trans_date - trans1.trans_date) < INTERVAL '20' MINUTE
                                                     )
                      THEN 'Попытка подбора суммы'
                      ELSE 'Корректная транзакция'
                      END AS event_type,
                      TO_DATE(?, 'YYYY-MM-DD') AS report_dt
                      FROM  de2tm.ramu_dwh_fact_transactions trans
                      LEFT JOIN de2tm.ramu_dwh_dim_cards_hist cards
                          ON trans.card_num = RTRIM(cards.card_num)
                      LEFT JOIN de2tm.ramu_dwh_dim_accounts_hist accnt
                          ON cards.account_num = accnt.account_num
                      LEFT JOIN de2tm.ramu_dwh_dim_clients_hist clnts
                          ON accnt.client = clnts.client_id
                      WHERE trans.trans_date > TO_DATE(?, 'YYYY-MM-DD')
                      )
                      WHERE event_type != 'Корректная транзакция' 
                      ORDER BY event_dt""", (terminals_dt_str, terminals_dt_str, terminals_dt_1_str, terminals_dt_str, ))

conn.commit()


# Сохраняем отчет

fraud_df=pd.read_sql("""SELECT * FROM de2tm.ramu_rep_fraud""", conn)

fraud_df.to_excel('/home/de2tm/RAMU/report_fraud.xlsx', sheet_name ='Sheet_1', header=True, index=False)


curs.close()
conn.close()

# Backup
os.replace(pass_black, os.path.join('/home/de2tm/RAMU/archive', os.path.basename(pass_black + '.backup')))
os.replace(terminals, os.path.join('/home/de2tm/RAMU/archive', os.path.basename(terminals + '.backup')))
os.replace(transactions, os.path.join('/home/de2tm/RAMU/archive', os.path.basename(transactions + '.backup')))