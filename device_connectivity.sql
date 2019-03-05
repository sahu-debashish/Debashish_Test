spool device_connectivity.log

CREATE OR REPLACE PACKAGE saie_code.load_device_adopt_metrics_pkg
   AUTHID DEFINER
AS
   /*
      06-Feb-2019 : Initial Draft
   */

    PROCEDURE update_process_job_last_syncup (
        p_sync_status             IN     saie_config.process_job_last_sync_up.sync_status%TYPE,
        p_start_seq_num           IN     saie_config.process_job_last_sync_up.start_seq_num%TYPE,
        p_start_time              IN     saie_config.process_job_last_sync_up.start_time%TYPE,
        p_end_seq_num             IN     saie_config.process_job_last_sync_up.end_seq_num%TYPE,
        p_end_time                IN     saie_config.process_job_last_sync_up.end_time%TYPE,
        p_sync_name               IN     saie_config.process_job_last_sync_up.sync_name%TYPE,
        p_error_code              OUT    NUMBER,
        p_error_message           OUT    VARCHAR2);
        
    PROCEDURE device_adoption_metrics_proc      (p_error_code      OUT VARCHAR2,
                                              p_error_message   OUT VARCHAR2);
    

END;
/

CREATE OR REPLACE PACKAGE BODY saie_code.load_device_adopt_metrics_pkg
AS
   /*
      06-Feb-2019 : Initial Draft
   */

    PROCEDURE update_process_job_last_syncup (
        p_sync_status             IN     saie_config.process_job_last_sync_up.sync_status%TYPE,
        p_start_seq_num           IN     saie_config.process_job_last_sync_up.start_seq_num%TYPE,
        p_start_time              IN     saie_config.process_job_last_sync_up.start_time%TYPE,
        p_end_seq_num             IN     saie_config.process_job_last_sync_up.end_seq_num%TYPE,
        p_end_time                IN     saie_config.process_job_last_sync_up.end_time%TYPE,
        p_sync_name               IN     saie_config.process_job_last_sync_up.sync_name%TYPE,
        p_error_code              OUT    NUMBER,
        p_error_message           OUT    VARCHAR2)
    
    AS
        pragma autonomous_transaction;
        l_sync_up_id NUMBER;
    
    BEGIN
    
        p_error_code := '0';
        p_error_message := 'Success';        
    
        SELECT sync_up_id INTO l_sync_up_id FROM saie_config.process_job_last_sync_up WHERE sync_name = p_sync_name;
    
        UPDATE saie_config.process_job_last_sync_up
        SET sync_up_date = SYSDATE,
            sync_status = p_sync_status,
            update_date = SYSTIMESTAMP,
            update_by = USER,
            start_seq_num = NVL (p_start_seq_num, start_seq_num),
            start_time = NVL (p_start_time, start_time),
            end_seq_num = NVL (p_end_seq_num, end_seq_num),
            end_time = NVL (p_end_time, end_time)
        WHERE sync_up_id = l_sync_up_id;
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS 
        THEN
            
            ROLLBACK;
            p_error_code := 1;
            p_error_message  := 'Error for procedure saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup ' || p_sync_name || '. ' || SUBSTR (SQLERRM, 1, 3500);
            
    END update_process_job_last_syncup;
    


    PROCEDURE device_adoption_metrics_proc       (p_error_code      OUT VARCHAR2,
                                                  p_error_message   OUT VARCHAR2)
    IS
      
        l_bulk                      NUMBER;

        l_error_level               NUMBER;

        l_sync_name                 saie_config.process_job_last_sync_up.sync_name%TYPE;
                                    
        l_error_code                NUMBER;
                                    
        l_error_message             VARCHAR2 (4000 CHAR);
        
        l_in_device_adoption_metrics    saie_inbound.in_device_adoption_con_metrics%ROWTYPE;
        
        l_device_adoption_metrics       saie_reporting.device_adoption_conn_metrics%ROWTYPE;
        
        l_max_run_seq_num           saie_inbound.in_device_adoption_con_metrics.run_seq_num%TYPE;
        
        l_count                     NUMBER;
                                    
        l_error_exception           EXCEPTION;
                                    
        l_rec_exist                 NUMBER := '0';
        
        l_retention_days            NUMBER := '0';
        
        l_current_retention_date    DATE;
        
        l_current_partition_date    DATE;
        
        v_sql                       VARCHAR2(4000 CHAR);
                                    
        CURSOR device_adopt_conn_met
        IS
           SELECT run_seq_num, src_run_seq_num, bu_id, item_class_code, base_prod_code, local_channel_code, event_source, device_count, unq_device_count, connectivity_count, calendar_date, calendar_type, create_date, create_by FROM saie_inbound.in_device_adoption_con_metrics WHERE run_seq_num > l_in_device_adoption_metrics.run_seq_num AND run_seq_num <= l_in_device_adoption_metrics.run_seq_num + l_bulk ORDER BY run_seq_num;
           
        CURSOR curr_partition
           IS
              SELECT table_owner, table_name, partition_name, high_value from all_tab_partitions WHERE table_name = 'IN_DEVICE_ADOPTION_CON_METRICS' AND table_owner = 'SAIE_INBOUND';
           
    BEGIN
        
        p_error_code := 0;

        p_error_message := 'Success';
        
        l_bulk := 10000;
        
        l_sync_name := 'SAIE_ANALYTICS_device_adpt_count_met';
        
        BEGIN
           l_error_level := '1.1';
        
           saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup ( 'ACTIVE', NULL, NULL, NULL, NULL, l_sync_name, l_error_code, l_error_message);
        
           l_error_level := '1.2';
        
           IF (l_error_code <> '0')
           THEN
              
              l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || l_error_message;
        
              RAISE l_error_exception;
           END IF;
        EXCEPTION
           WHEN OTHERS
           THEN
              
              l_error_code := '1';
              l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
        
              RAISE l_error_exception;
        END;
        
        BEGIN
           l_error_level := '1.7';
        
           SELECT NVL (prop_value, '0') INTO l_retention_days FROM saie_config.sys_props WHERE prop_source = 'SAIE_ANALYTICS_device_adpt_count_met' AND prop_name = 'TRUNC_device_adopt_conn_met';
        
           l_error_level := '1.8';
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_retention_days := '0';
        
           WHEN OTHERS
           THEN
              l_retention_days := '0';
        
              l_error_code := '1';
              l_error_message := 'Error while getting retention_day value from sys_props. Error Level : ' || l_error_level || SUBSTR (SQLERRM, 1, 3500);
        
              RAISE l_error_exception;
        END;
        
        l_current_retention_date := TRUNC (SYSDATE - l_retention_days);
        
        l_error_level := '1.9';

        BEGIN
           l_error_level := '2.1';
        
           SELECT NVL (MAX (run_seq_num), '0') INTO l_max_run_seq_num FROM saie_inbound.in_device_adoption_con_metrics;
        
           l_error_level := '2.2';
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_max_run_seq_num := '0';
        
           WHEN OTHERS
           THEN
              l_max_run_seq_num := '0';
        
              l_error_code := '1';
              l_error_message := 'Error while getting NVL(MAX(run_seq_num),''0'') value. Error Level : ' || l_error_level || '. max_run_seq_num = ' || l_max_run_seq_num || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
        
              RAISE l_error_exception;
        END;
        
        BEGIN
           l_error_level := '3.1';
        
           SELECT NVL (start_seq_num, '0') INTO l_in_device_adoption_metrics.run_seq_num FROM saie_config.process_job_last_sync_up WHERE sync_name = l_sync_name;
        
           l_error_level := '3.2';
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_in_device_adoption_metrics.run_seq_num := '0';
        
              l_error_code := '1';
              l_error_message := 'No records exists saie_config.process_job_last_sync_up for sync_name ' || l_sync_name || '. Error Level : ' || l_error_level || '. run_seq_num = ' || l_in_device_adoption_metrics.run_seq_num || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
        
              RAISE l_error_exception;
           WHEN OTHERS
           THEN
              l_in_device_adoption_metrics.run_seq_num := '0';
        
              l_error_code := '1';
              l_error_message := 'Error while reading data from saie_config.process_job_last_sync_up for sync_name ' || l_sync_name || '. Error Level : ' || l_error_level || '. run_seq_num = ' || l_in_device_adoption_metrics.run_seq_num || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
        
              RAISE l_error_exception;
        END;

        WHILE (l_max_run_seq_num > l_in_device_adoption_metrics.run_seq_num)
        LOOP
            BEGIN
                saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup ( 'INPROGRESS', NULL, NULL, NULL, NULL, l_sync_name, l_error_code, l_error_message);
                
                l_error_level := '1.3';
                
                IF (l_error_code <> '0')
                THEN
                   
                    l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || l_error_message;
                    
                    RAISE l_error_exception;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  
                    l_error_code := '1';
                    l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
            
                  RAISE l_error_exception;
            END;
            
            l_rec_exist := '0';
            
            FOR i IN device_adopt_conn_met
            LOOP
            
                l_error_level := '4.1';
            
                l_in_device_adoption_metrics := NULL;
                
                l_error_level := '4.2';
                
                l_device_adoption_metrics := NULL;
                
                l_error_level := '4.3';
            
                l_rec_exist := '1';
                
                l_error_level := '4.4';
                
                l_in_device_adoption_metrics.run_seq_num := i.run_seq_num;
                
                l_error_level := '4.5';
                
                /* daily load */
                
                BEGIN
                   
                   SELECT COUNT(*) INTO l_count FROM saie_reporting.device_adoption_conn_metrics WHERE calendar_type = i.calendar_type AND calendar_id = i.calendar_date 
                   AND bu_id = i.bu_id AND item_class_code = i.item_class_code AND local_channel_code = i.local_channel_code AND base_prod_code = i.base_prod_code 
                   AND event_source = i.event_source;
                
                   l_error_level := '4.5';
                EXCEPTION
                   WHEN NO_DATA_FOUND
                   THEN
                      
                      l_count := '0';
                      
                   WHEN OTHERS
                   THEN
                      
                      l_count := '0';
                
                      l_error_code := '1';
                      l_error_message := 'Error while reading data from saie_reporting.device_adoption_conn_metrics ' || l_in_device_adoption_metrics.run_seq_num || '. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
                
                      RAISE l_error_exception;
                END;
                
                IF ( NVL (l_count, '0') = '0' ) THEN
                
                    l_error_level := '4.6';
                
                    BEGIN
                    
                        INSERT INTO saie_reporting.device_adoption_conn_metrics (src_run_seq_num, bu_id, item_class_code, base_prod_code, local_channel_code, event_source,  device_count, unq_device_count, connectivity_count, calendar_id, calendar_type, create_date, create_by, update_date, update_by)
                        VALUES
                        (i.src_run_seq_num, i.bu_id, i.item_class_code, i.base_prod_code, i.local_channel_code, i.event_source, i.device_count, i.unq_device_count, i.connectivity_count, i.calendar_date, i.calendar_type, SYSTIMESTAMP, USER, SYSTIMESTAMP, USER);
                    
                    EXCEPTION
                    
                        WHEN OTHERS
                        THEN
                        l_error_code := '1';
                        l_error_message := 'Error while inserting into saie_reporting.device_adoption_conn_metrics ' || l_in_device_adoption_metrics.run_seq_num || '. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
                        
                        RAISE l_error_exception;
                    
                    END;
                
                ELSIF (l_count > '0' ) THEN
                
                    l_error_level := '4.8';
                    
                    BEGIN
                    
                        UPDATE saie_reporting.device_adoption_conn_metrics 
                            SET src_run_seq_num = i.src_run_seq_num,
                                device_count = i.device_count,
                                unq_device_count = i.unq_device_count,
                                connectivity_count = i.connectivity_count,
                                update_date = SYSTIMESTAMP,
                                update_by = USER
                            WHERE bu_id = i.bu_id
                              AND item_class_code = i.item_class_code
                              AND base_prod_code = i.base_prod_code
                              AND local_channel_code = i.local_channel_code
                              AND event_source = i.event_source
                              AND calendar_id = i.calendar_date
                              AND calendar_type = i.calendar_type;
                    EXCEPTION
                    
                        WHEN OTHERS
                        THEN
                        l_error_code := '1';
                        l_error_message := 'Error while updating the saie_reporting.device_adoption_conn_metrics ' || l_in_device_adoption_metrics.run_seq_num || '. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
                        
                        RAISE l_error_exception;
                    
                    END;
                
                END IF;
                
            END LOOP;
            
            IF (NVL (l_rec_exist, '-1') <= '0')
            THEN
            
                SELECT MIN (run_seq_num) - 1 INTO l_in_device_adoption_metrics.run_seq_num FROM saie_inbound.in_device_adoption_con_metrics WHERE run_seq_num > l_in_device_adoption_metrics.run_seq_num;
                                
            END IF;
            
            BEGIN
   
                l_error_level := '1.4';
                
                UPDATE saie_config.process_job_last_sync_up
                    SET sync_up_date = SYSDATE,
                        sync_status = 'INPROGRESS',
                        update_date = SYSTIMESTAMP,
                        update_by = USER,
                        start_seq_num = NVL (l_in_device_adoption_metrics.run_seq_num, start_seq_num)
                    WHERE sync_name = l_sync_name;
                    
                l_error_level := '1.5';
            
            EXCEPTION
                WHEN OTHERS
                THEN
               
                    l_error_code := '1';
                    l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
                    
                    RAISE l_error_exception;
            
            END;
               
            COMMIT;
        END LOOP;
        
        IF (l_retention_days >= 15) THEN
        
            FOR i IN curr_partition
            LOOP
                l_error_level := '6';
            
                v_sql := 'SELECT ' || i.high_value || ' FROM dual';
            
                EXECUTE IMMEDIATE v_sql INTO l_current_partition_date;
            
                BEGIN
            
                    IF (l_current_partition_date < l_current_retention_date)
                    THEN
                        l_error_level := '6.1';
                      
                        saie_inbound.inbound_code_utilities_pkg.trunc_table_partition (i.table_owner, i.table_name, i.partition_name ,l_error_code,l_error_message);
                        
                        IF (l_error_code <> '0')
                        THEN
                           
                            l_error_message := 'Error while truncating the partition ' || i.partition_name || '. Error Level : ' || l_error_level || '. Error ' || l_error_message;
                            
                            RAISE l_error_exception;
                        END IF;
                      
                    END IF;
                
            EXCEPTION WHEN OTHERS THEN
            
                    l_error_code := '1';
                    l_error_message := 'Error in saie_inbound.inbound_code_utilities_pkg.trunc_table_partition. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
                    
                    RAISE l_error_exception;
                
            END;
                
            END LOOP;
            
        END IF;
         
        BEGIN
            saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup ( 'COMPLETED', l_in_device_adoption_metrics.run_seq_num, NULL, NULL, NULL, l_sync_name, l_error_code, l_error_message);
   
            l_error_level := '1.6';
         
            IF (l_error_code <> '0')
            THEN
               
               l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || l_error_message;
         
               RAISE l_error_exception;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
               
               l_error_code := '1';
               l_error_message := 'Error in saie_code.load_device_adopt_metrics_pkg.update_process_job_last_syncup. Error Level : ' || l_error_level || '. Error ' || SUBSTR (SQLERRM, 1, 3500);
         
               RAISE l_error_exception;
        END;
         
        COMMIT;

    EXCEPTION
        WHEN l_error_exception 
        THEN
            ROLLBACK;
            p_error_code := l_error_code;
            p_error_message := l_error_message;
            
            INSERT INTO saie_audit.err_device_adoption_metrics ( run_seq_num, err_event_date, ERROR_CODE, error_message, create_date, update_date, create_by, update_by)
            VALUES 
            (l_in_device_adoption_metrics.run_seq_num, SYSTIMESTAMP, p_error_code, p_error_message, SYSTIMESTAMP, SYSTIMESTAMP, USER, USER);
            
            COMMIT;
    
        WHEN OTHERS
        THEN

            ROLLBACK;      
            p_error_code := '1';
            p_error_message := 'Error in load_device_adopt_metrics_pkg.device_adoption_metrics. Error Level : ' || l_error_level || '. Run_seq_num : ' || l_in_device_adoption_metrics.run_seq_num || '. ERROR ' || SUBSTR (SQLERRM, 1, 3500);
         
            INSERT INTO saie_audit.err_device_adoption_metrics ( run_seq_num, err_event_date, ERROR_CODE, error_message, create_date, update_date, create_by, update_by)
            VALUES 
            (l_in_device_adoption_metrics.run_seq_num, SYSTIMESTAMP, p_error_code, p_error_message, SYSTIMESTAMP, SYSTIMESTAMP, USER, USER);
            
            COMMIT;
         
    END device_adoption_metrics_proc;
END load_device_adopt_metrics_pkg;
/

spool off;
