package org.infinity.classes;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.infinity.classes.DatabaseConnection.tableExists;

public class CSVReader {
    public static void readAndStoreATRData(String filepath) throws Exception {
        try(BufferedReader br = new BufferedReader(new FileReader(filepath));
        Connection connection = DatabaseConnection.getConnection()){
            String line;
            boolean isFirstLine = true; // Flag to ignore the first line

            while ((line = br.readLine()) != null){
                String[] data = line.split(";");
                if (isFirstLine) {
                    isFirstLine = false;
                    continue; // Skip processing the first line
                }
                if (!tableExists(connection,"ATRTable")){
                    String query = "CREATE TABLE ATRTable (id SERIAL PRIMARY KEY, card_number VARCHAR(255), expiration_date VARCHAR(255), card_id VARCHAR(255), token_id VARCHAR(255), cbs_card_number VARCHAR(255), date VARCHAR(255), time VARCHAR(255), mti VARCHAR(255), txn_condition VARCHAR(255), pos_entry_mode VARCHAR(255), pin_capability VARCHAR(255), orig_txn_amount VARCHAR(255), orig_txn_curr VARCHAR(255), auth_amount VARCHAR(255), auth_curr VARCHAR(255), reversal_amount VARCHAR(255), reversal_curr VARCHAR(255), reversal_reason VARCHAR(255), resp_code VARCHAR(255), pin_ind VARCHAR(255), auth_id VARCHAR(255), mcc VARCHAR(255), acq_id VARCHAR(255), country VARCHAR(255), city VARCHAR(20), merchant_name VARCHAR(20), caid VARCHAR(20), source_reg_number VARCHAR(20), terminal_id VARCHAR(20), merchant_location VARCHAR(20), atc VARCHAR(255), terminal_capability_profile VARCHAR(255), terminal_verification_results VARCHAR(255), card_verification_results VARCHAR(255), originator VARCHAR(255), responder VARCHAR(255), condition VARCHAR(255), condition_details VARCHAR(255), transaction_code VARCHAR(255), trid VARCHAR(255), wallet_identifier VARCHAR(255), txref VARCHAR(255), auth_record_id VARCHAR(255), auth_duration VARCHAR(255))";
                    connection.createStatement().execute(query);
                    System.out.println("Table 'ATRTable' created successfully.");
                }else{
                    System.out.println("Table 'ATRTable' already exists.");
                }

                String query = "INSERT INTO ATRTable (card_number, expiration_date, card_id, token_id, cbs_card_number, date, time, mti, txn_condition, pos_entry_mode, pin_capability, orig_txn_amount, orig_txn_curr, auth_amount, auth_curr, reversal_amount, reversal_curr, reversal_reason, resp_code, pin_ind, auth_id, mcc, acq_id, country, city, merchant_name, caid, source_reg_number, terminal_id, merchant_location, atc, terminal_capability_profile, terminal_verification_results, card_verification_results, originator, responder, condition, condition_details, transaction_code, trid, wallet_identifier, txref, auth_record_id, auth_duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                preparedStatement.setString(1, data[0]);
                preparedStatement.setString(2, data[1]);
                preparedStatement.setString(3, data[2]);
                preparedStatement.setString(4, data[3]);
                preparedStatement.setString(5, data[4]);
                preparedStatement.setString(6, data[5]);
                preparedStatement.setString(7, data[6]);
                preparedStatement.setString(8, data[7]);
                preparedStatement.setString(9, data[8]);
                preparedStatement.setString(10, data[9]);
                preparedStatement.setString(11, data[10]);
                preparedStatement.setString(12, data[11]);
                preparedStatement.setString(13, data[12]);
                preparedStatement.setString(14, data[13]);
                preparedStatement.setString(15, data[14]);
                preparedStatement.setString(16, data[15]);
                preparedStatement.setString(17, data[16]);
                preparedStatement.setString(18, data[17]);
                preparedStatement.setString(19, data[18]);
                preparedStatement.setString(20, data[19]);
                preparedStatement.setString(21, data[20]);
                preparedStatement.setString(22, data[21]);
                preparedStatement.setString(23, data[22]);
                preparedStatement.setString(24, data[23]);
                preparedStatement.setString(25, data[24]);
                preparedStatement.setString(26, data[25]);
                preparedStatement.setString(27, data[26]);
                preparedStatement.setString(28, data[27]);
                preparedStatement.setString(29, data[28]);
                preparedStatement.setString(30, data[29]);
                preparedStatement.setString(31, data[30]);
                preparedStatement.setString(32, data[31]);
                preparedStatement.setString(33, data[32]);
                preparedStatement.setString(34, data[33]);
                preparedStatement.setString(35, data[34]);
                preparedStatement.setString(36, data[35]);
                preparedStatement.setString(37, data[36]);
                preparedStatement.setString(38, data[37]);
                preparedStatement.setString(39, data[38]);
                preparedStatement.setString(40, data[39]);
                preparedStatement.setString(41, data[40]);
                preparedStatement.setString(42, data[41]);
                preparedStatement.setString(43, data[42]);
                preparedStatement.setString(44, data[43]);
                preparedStatement.executeUpdate();
            }
            System.out.println("Data inserted successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
