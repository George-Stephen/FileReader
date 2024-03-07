package org.infinity.classes;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class CSVReader {
    public static void readAndStoreATRData(String filepath) throws Exception {
        try(BufferedReader br = new BufferedReader(new FileReader(filepath));
        Connection connection = DatabaseConnection.getConnection()){
            String line;
            while ((line = br.readLine()) != null){
                String[] data = line.split(";");
                String query = "INSERT INTO ATRTable (card_number, expiration_date, card_id, token_id, cbs_card_number, date, time, mti, txn_condition, pos_entry_mode, pin_capability, orig_txn_amount, orig_txn_curr, auth_amount, auth_curr, reversal_amount, reversal_curr, reversal_reason, resp_code, pin_ind, auth_id, mcc, acq_id, country, city, merchant_name, caid, source_reg_number, terminal_id, merchant_location, atc, terminal_capability_profile, terminal_verification_results, card_verification_results, originator, responder, condition, condition_details, transaction_code, trid, wallet_identifier, txref, auth_record_id, auth_duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                preparedStatement.setString(1, data[0]);
                preparedStatement.setString(2, data[1]);
                preparedStatement.setLong(3, Long.parseLong(data[2]));
                preparedStatement.setLong(4, Long.parseLong(data[3]));
                preparedStatement.setString(5, data[4]);
                preparedStatement.setString(6, data[5]);
                preparedStatement.setString(7, data[6]);
                preparedStatement.setString(8, data[7]);
                preparedStatement.setString(9, data[8]);
                preparedStatement.setInt(10, Integer.parseInt(data[9]));
                preparedStatement.setInt(11, Integer.parseInt(data[10]));
                preparedStatement.setString(12, data[11]);
                preparedStatement.setInt(13, Integer.parseInt(data[12]));
                preparedStatement.setString(14, data[13]);
                preparedStatement.setInt(15, Integer.parseInt(data[14]));
                preparedStatement.setString(16, data[15]);
                preparedStatement.setString(17, data[16]);
                preparedStatement.setInt(18, Integer.parseInt(data[17]));
                preparedStatement.setString(19, data[18]);
                preparedStatement.setInt(20, Integer.parseInt(data[19]));
                preparedStatement.setString(21, data[20]);
                preparedStatement.setInt(22, Integer.parseInt(data[21]));
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
                preparedStatement.setInt(37, Integer.parseInt(data[36]));
                preparedStatement.setString(38, data[37]);
                preparedStatement.setInt(39, Integer.parseInt(data[38]));
                preparedStatement.setLong(40, Long.parseLong(data[39]));
                preparedStatement.setString(41, data[40]);
                preparedStatement.setString(42, data[41]);
                preparedStatement.setString(43, data[42]);
                preparedStatement.setString(44, data[43]);
                preparedStatement.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } ;

    }
}
