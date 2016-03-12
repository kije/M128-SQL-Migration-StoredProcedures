-- Setup a database only for the procedures, so we later do not need to worry in which db they are etc...
DROP DATABASE IF EXISTS __school_migration_procedures__;
CREATE DATABASE __school_migration_procedures__;

USE __school_migration_procedures__;

-- procedures
DELIMITER //

# helper procedure for calling dynamic sql queries
DROP PROCEDURE IF EXISTS __school_migration_procedures__.eval_sql//
CREATE PROCEDURE __school_migration_procedures__.eval_sql(IN sql_stmt TEXT)
DETERMINISTIC
  BEGIN
    -- use prepared statement to execute dynamic sql (a sql statement from a string)
    SET @sql = sql_stmt;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END //


# clean up data (to ensure referential integrity)
DROP PROCEDURE IF EXISTS __school_migration_procedures__.cleanup_old_data//
CREATE PROCEDURE __school_migration_procedures__.cleanup_old_data()
DETERMINISTIC
  BEGIN
    -- vars
    DECLARE from_tablename VARCHAR(255) DEFAULT '';
    DECLARE to_tablename VARCHAR(255) DEFAULT '';
    DECLARE from_column_name VARCHAR(255) DEFAULT '';
    DECLARE to_column_name VARCHAR(255) DEFAULT '';
    DECLARE from_id_column_name VARCHAR(255) DEFAULT '';
    DECLARE null_allowed BOOLEAN DEFAULT FALSE;
    DECLARE finished BOOLEAN DEFAULT FALSE;

    -- cursor
    DECLARE get_mapping_cursor CURSOR FOR
      SELECT
        from_table,
        from_column,
        from_id_column,
        to_table,
        to_column,
        `null`
      FROM __school_migration_procedures__.tmp_foreign_key_mapping;

    -- declare NOT FOUND handler
    DECLARE CONTINUE HANDLER
    FOR NOT FOUND SET finished = TRUE;

    -- tmp table for mapping
    CREATE TEMPORARY TABLE IF NOT EXISTS __school_migration_procedures__.tmp_foreign_key_mapping (
      from_table     VARCHAR(255),
      from_column    VARCHAR(255),
      from_id_column VARCHAR(255),
      to_table       VARCHAR(255),
      to_column      VARCHAR(255),
      `null`         BOOLEAN DEFAULT FALSE
    )
      ENGINE = MEMORY
      CHAR SET 'utf8';

    -- order is important
    INSERT INTO __school_migration_procedures__.tmp_foreign_key_mapping VALUES
      ('schueler', 'lehrbetrieb_id', 'id', 'lehrbetrieb', 'id', TRUE),
      ('schueler', 'richtung_id', 'id', 'richtung', 'id', TRUE),
      ('schueler', 'klasse_id', 'id', 'klasse', 'id', FALSE),
      ('note', 'schueler_id', 'id', 'schueler', 'id', FALSE),
      ('note', 'modul_id', 'id', 'modul', 'id', FALSE);

    -- clean data
    OPEN get_mapping_cursor;

    clean_loop: LOOP
      FETCH get_mapping_cursor
      INTO from_tablename, from_column_name, from_id_column_name, to_tablename, to_column_name, null_allowed;

      -- if finished, exit
      IF finished = TRUE
      THEN
        LEAVE clean_loop;
      END IF;

      -- sql statement for finding rows with no entry in the referenced table
      SET @select_sql = CONCAT(
          'SELECT `', to_column_name, '` FROM schoolinfo_neu.`', to_tablename, '`'
      );


      SET @delete_sql = CONCAT(
          'DELETE FROM schoolinfo_neu.`', from_tablename, '` WHERE `', from_column_name, '` NOT IN (
         ', @select_sql, '
        );
        '
      );

      CALL __school_migration_procedures__.eval_sql(@delete_sql);

    END LOOP;

    DROP TEMPORARY TABLE __school_migration_procedures__.tmp_foreign_key_mapping;
  END //

# copy database
DROP PROCEDURE IF EXISTS __school_migration_procedures__.copy_database//
CREATE PROCEDURE __school_migration_procedures__.copy_database()
DETERMINISTIC
  BEGIN
    -- vars
    DECLARE tablename VARCHAR(255) DEFAULT '';
    DECLARE finished BOOLEAN DEFAULT FALSE;

    -- cursor
    DECLARE get_old_tables_cursor CURSOR FOR
      SELECT `table_name`
      FROM `information_schema`.`tables`
      WHERE `table_schema` = 'schoolinfo1282016';

    -- declare NOT FOUND handler
    DECLARE CONTINUE HANDLER
    FOR NOT FOUND SET finished = TRUE;

    -- setup database
    DROP DATABASE IF EXISTS schoolinfo_neu;
    CREATE DATABASE schoolinfo_neu;
    ALTER DATABASE schoolinfo_neu
    CHARACTER SET utf8
    COLLATE utf8_unicode_ci;

    -- copy schema
    OPEN get_old_tables_cursor;

    migration_loop: LOOP
      FETCH get_old_tables_cursor
      INTO tablename;

      -- if finished, exit
      IF finished = TRUE
      THEN
        LEAVE migration_loop;
      END IF;


      SET @sql = CONCAT(
          'CREATE TABLE ',
          'schoolinfo_neu', -- new database
          '.',
          tablename,
          ' LIKE ',
          'schoolinfo1282016', -- old database
          '.',
          tablename
      );

      CALL __school_migration_procedures__.eval_sql(@sql);

      -- make sure, new tables are of type InnoDB and encoding is utf8
      SET @sql = CONCAT(
          'ALTER TABLE ',
          'schoolinfo_neu', -- new database
          '.',
          tablename,
          ' ENGINE=InnoDB, '
          'CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci'
      );

      CALL __school_migration_procedures__.eval_sql(@sql);

      -- todo transaction!!!
      -- copy data
      SET @sql = CONCAT(
          'INSERT INTO ',
          'schoolinfo_neu', -- new database
          '.',
          tablename,
          ' SELECT DISTINCT * FROM ',
          'schoolinfo1282016', -- old database
          '.',
          tablename
      );

      CALL __school_migration_procedures__.eval_sql(@sql);

    END LOOP;

    CLOSE get_old_tables_cursor;
  END //

# copy database
DROP PROCEDURE IF EXISTS __school_migration_procedures__.add_auto_increment//
CREATE PROCEDURE __school_migration_procedures__.add_auto_increment()
DETERMINISTIC
  BEGIN
    -- vars
    DECLARE tablename VARCHAR(255) DEFAULT '';
    DECLARE finished BOOLEAN DEFAULT FALSE;

    DECLARE get_new_tables_cursor CURSOR FOR
      SELECT `table_name`
      FROM `information_schema`.`tables`
      WHERE `table_schema` = 'schoolinfo_neu';

    -- declare NOT FOUND handler
    DECLARE CONTINUE HANDLER
    FOR NOT FOUND SET finished = TRUE;

    OPEN get_new_tables_cursor;

    auto_increment_loop: LOOP
      FETCH get_new_tables_cursor
      INTO tablename;

      -- if finished, exit
      IF finished = TRUE
      THEN
        LEAVE auto_increment_loop;
      END IF;

      -- find tables witch has id starting at 0
      SET @sql = CONCAT(
          'SELECT count(id) INTO @count_zero_id FROM  schoolinfo_neu.`', tablename, '` WHERE id = 0;'
      );

      CALL __school_migration_procedures__.eval_sql(@sql);


      IF @count_zero_id > 0
      THEN
        -- increment all id's by 1, so ids start with 1 and we can add auto increment
        -- note the ORDER BY ID DESC. This ensures that the highest ids will be incremented first, so we do not get duplicate entry for primary key
        SET @sql = CONCAT(
            'UPDATE  schoolinfo_neu.`', tablename, '` SET id = id + 1 ORDER BY ID DESC;'
        );

        CALL __school_migration_procedures__.eval_sql(@sql);
      END IF;

      -- get colum definition for alter table
      SELECT `column_type`
      INTO @col_definition
      FROM `information_schema`.`columns`
      WHERE `table_schema` = 'schoolinfo_neu' AND
            `table_name` = tablename AND
            `column_name` = 'id';

      -- add auto increment
      -- we need to temporarily disable foreign key checks in order to update a column definition referenced by a foreign key
      -- note: this should not have any side effects, because we do not modify data or change the data type of the column
      SET FOREIGN_KEY_CHECKS = 0;
      SET @sql = CONCAT(
          'ALTER TABLE schoolinfo_neu.`', tablename, '` MODIFY COLUMN id ', @col_definition, ' AUTO_INCREMENT;'
      );

      CALL __school_migration_procedures__.eval_sql(@sql);

      SET FOREIGN_KEY_CHECKS = 1;

    END LOOP;

    CLOSE get_new_tables_cursor;
  END //

# migrate db structure
DROP PROCEDURE IF EXISTS __school_migration_procedures__.migrate_schema//
CREATE PROCEDURE __school_migration_procedures__.migrate_schema()
DETERMINISTIC
  BEGIN
    -- copy db
    CALL `__school_migration_procedures__`.copy_database();

    -- todo rearange columns
    -- make selective changes to columns on individual tables
    -- table klasse
    ALTER TABLE schoolinfo_neu.klasse
    CHANGE COLUMN idklasse id INT UNSIGNED,
    CHANGE `name` short_name VARCHAR(10) NOT NULL,
    CHANGE realname `name` VARCHAR(45) NULL
    AFTER short_name;

    -- table lehrbetriebe -> lehrbetrieb
    RENAME TABLE schoolinfo_neu.lehrbetriebe TO schoolinfo_neu.lehrbetrieb;
    ALTER TABLE schoolinfo_neu.lehrbetrieb
    CHANGE COLUMN id_Lehrbetrieb id INT UNSIGNED,
    CHANGE COLUMN FName `name` VARCHAR(255) NOT NULL,
    CHANGE COLUMN FStrasse `strasse` VARCHAR(255) NULL,
    CHANGE COLUMN FHausNr `haus_nr` VARCHAR(255) NULL,
    CHANGE COLUMN FPlz `plz` VARCHAR(10) NULL,
    CHANGE COLUMN FOrt `ort` VARCHAR(255) NULL,
    CHANGE COLUMN FKanton `kanton` CHAR(2) NULL,
    CHANGE COLUMN FLand `land` VARCHAR(255) NULL,
    ADD INDEX `idx_lehrbetrieb_name` (`name`),
    ADD INDEX `idx_lehrbetrieb_adresse` (`strasse`, `haus_nr`),
    ADD INDEX `idx_lehrbetrieb_adresse2` (`land`, `kanton`);

    -- table lernende -> schueler
    RENAME TABLE schoolinfo_neu.lernende TO schoolinfo_neu.schueler;
    ALTER TABLE schoolinfo_neu.schueler
    CHANGE COLUMN Lern_id id INT UNSIGNED,
    CHANGE COLUMN lehrbetrieb lehrbetrieb_id INT UNSIGNED NULL,
    CHANGE COLUMN richtung richtung_id INT UNSIGNED NOT NULL,
    CHANGE COLUMN klasse klasse_id INT UNSIGNED NOT NULL,
    MODIFY COLUMN `name` VARCHAR(50) NOT NULL,
    MODIFY COLUMN `vorname` VARCHAR(50) NOT NULL,
    DROP INDEX Lern_id,
    DROP INDEX vorname,
    DROP INDEX `name`;

    -- fix boolean flag
    UPDATE schoolinfo_neu.schueler
    SET bm = 1
    WHERE bm = -1;

    ALTER TABLE schoolinfo_neu.schueler
    MODIFY COLUMN bm TINYINT(1) UNSIGNED NOT NULL DEFAULT '0';

    -- gender
    UPDATE schoolinfo_neu.schueler
      SET geschlecht = 'm'
      WHERE geschlecht REGEXP '^(m|M)';

    UPDATE schoolinfo_neu.schueler
      SET geschlecht = 'w'
      WHERE geschlecht REGEXP '^(w|W)';

    ALTER TABLE schoolinfo_neu.schueler
      MODIFY COLUMN geschlecht ENUM('m', 'w') NULL;

    -- table modul
    ALTER TABLE schoolinfo_neu.modul
    CHANGE COLUMN idmodul id INT UNSIGNED,
    CHANGE COLUMN m_name module_short_name VARCHAR(30) NOT NULL
    AFTER modul_name,
    CHANGE COLUMN modulname modul_name VARCHAR(100) NULL,
    DROP INDEX index2;

    -- table noten -> note
    RENAME TABLE schoolinfo_neu.noten TO schoolinfo_neu.note;
    ALTER TABLE schoolinfo_neu.note
    ADD COLUMN id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY
    FIRST,
    CHANGE COLUMN lernende_idlernende schueler_id INT UNSIGNED NOT NULL,
    CHANGE COLUMN module_idmodule modul_id INT UNSIGNED NOT NULL,
    CHANGE COLUMN dat_erfa datum_erfahrungs_note DATETIME NULL,
    CHANGE COLUMN dat_knw datum_knw_note DATETIME NULL,
    CHANGE COLUMN erfahrungsnote erfahrungs_note DECIMAL(2, 1) UNSIGNED NOT NULL,
    MODIFY COLUMN knw_note DECIMAL(2, 1) UNSIGNED NOT NULL,
    DROP INDEX lernende_idlernende;

    -- table richtung
    ALTER TABLE schoolinfo_neu.richtung
    CHANGE COLUMN idrichtung id INT UNSIGNED PRIMARY KEY,
    MODIFY COLUMN richtung VARCHAR(30) NOT NULL,
    DROP INDEX richtungrichtung;

    -- data clean up before setting foreign keys
    CALL __school_migration_procedures__.cleanup_old_data();

    -- add FOREIGN KEYS
    -- foreign keys for table note(schueler_id,schueler_id)
    ALTER TABLE schoolinfo_neu.note
    ADD CONSTRAINT note_schueler_id FOREIGN KEY (schueler_id) REFERENCES schueler (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE,
    -- unfortunately, the following foreign key constraint is not possible without modifying (deleting) data
    ADD CONSTRAINT note_modul_id FOREIGN KEY (modul_id) REFERENCES modul (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE;

    -- foreign key for table schueler(lehrbetrieb_id)
    ALTER TABLE schoolinfo_neu.schueler
    ADD CONSTRAINT schueler_richtung_id FOREIGN KEY (richtung_id) REFERENCES richtung (id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT, -- richtung can only be deleted, if there are no more linked schueler
    ADD CONSTRAINT schueler_klasse_id FOREIGN KEY (klasse_id) REFERENCES klasse (id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT, -- klasse can only be deleted, if there are no more linked schueler
    -- unfortunately, the following foreign key constraint is not possible without modifying (deleting) data
    ADD CONSTRAINT schueler_lehrbetrieb_id FOREIGN KEY (lehrbetrieb_id) REFERENCES lehrbetrieb (id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT; -- lehrbetrieb can only be deleted, if there are no more linked schueler


    -- add auto increment (this needs to be after adding foreign keys)
    CALL `__school_migration_procedures__`.add_auto_increment();
  END //


DELIMITER ;

CALL __school_migration_procedures__.migrate_schema();

