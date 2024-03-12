/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.technote.utils;

import org.infinity.classes.DatabaseConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author admin
 */
public class MultiTableTSVProcessor {

    final String[] clearingTotalsColumns = {
        "recordTypeID",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "recordID",
        "ARN",
        "originalRecordID",
        "transactionDescription",
        "additionalInformation",
        "functionCode",
        "messageReasonCode",
        "transactionCurrency",
        "settlementCurrency",
        "currencyExponents",
        "settlementIndicator",
        "reconciledFile",
        "memberActivity",
        "accountRange",
        "processingCode",
        "debitsTransactionAmountTxnCurrency",
        "creditsTransactionAmountTxnCurrency",
        "amountNetTransactionTxnCurrency",
        "debitsTransactionAmountRecCurrency",
        "creditsTransactionAmountRecCurrency",
        "debitsFeeAmountRecCurrency",
        "creditsFeeAmountRecCurrency",
        "amountNetTransactionRecCurrency",
        "amountNetFeeRecCurrency",
        "amountNetTotalRecCurrency",
        "debitsTransactionNumber",
        "creditsTransactionNumber",
        "totalTransactionNumber",
        "IPMMessageNumber",
        "IPMfileID"
    };

    final String[] feeCollectionsColumns = {
        "recordTypeID",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "recordID",
        "originalRecordID",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "originalAmount",
        "originalCurrency",
        "reconciliationAmount",
        "reconciliationCurrency",
        "conversionRateCurrency",
        "transactionDescription",
        "processingCode",
        "functionCode",
        "messageReasonCode",
        "dataRecord",
        "actionDate",
        "IPMMessageNumber",
        "IPMfileID",
        "additionalInformation"
    };

    final String[] cbColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "authRecordID",
        "originalRecordID",
        "RRN",
        "ARN",
        "authCode",
        "tokenRequestorID",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "terminalID",
        "transactionDescription",
        "functionCode",
        "messageReasonCode",
        "messageReasonDetails",
        "documentsAttached",
        "merchantID",
        "merchantName",
        "merchantCity",
        "merchantState",
        "merchantPostalCode",
        "merchantLocation",
        "merchantCountryCode",
        "MCC",
        "MCCDescription",
        "transactionAmount",
        "transactionCurrency",
        "transactionSign",
        "contractPostingAmount",
        "contractPostingCurrency",
        "IPMMessageNumber",
        "IPMfileID",
        "additionalInformation"
    };

    final String[] itColumnNames = {
        "recordTypeID",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "recordID",
        "contractNumber",
        "contractCBSNumber",
        "interestAmount",
        "interestAmountSign",
        "interestCurrencyCode",
        "transactionDescription",
        "additionalInformation",
        "cmsInterestDetails"
    };

    final String[] spColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "ARN",
        "authCode",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "contractPostingAmount",
        "contractPostingCurrency",
        "contractPostingSign",
        "cmsTransactionFeeDetails",
        "transactionDescription",
        "CMStranactionDescription",
        "additionalInformation"
    };

    final String[] mfColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "cardExpiryDate",
        "cmsMiscellaneousFeeDetails",
        "transactionDescription",
        "CMStranactionDescription",
        "additionalInformation"
    };

    final String[] mtColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "cardExpiryDate",
        "cmsTransactionFeeDetails",
        "transactionDescription",
        "CMStranactionDescription",
        "additionalInformation"
    };

    final String[] transactionFromSchemeColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "authRecordID",
        "originalRecordID",
        "RRN",
        "ARN",
        "authCode",
        "tokenRequestorID",
        "contractID",
        "contractNumber",
        "contractCBSNumber",
        "contractClientID",
        "mainContractID",
        "mainContractClientID",
        "cardExpiryDate",
        "merchantID",
        "merchantName",
        "merchantCity",
        "merchantState",
        "merchantPostalCode",
        "merchantLocation",
        "merchantCountryCode",
        "MCC",
        "MCCDescription",
        "transactionAmount",
        "transactionCurrency",
        "transactionSign",
        "reconciliationAmount",
        "reconciliationCurrency",
        "cardholderBillingAmount",
        "cardholderBillingCurrency",
        "reconciliationConversionRate",
        "cardholderBillingConversionRate",
        "contractPostingAmount",
        "contractPostingCurrency",
        "contractPostingSign",
        "cmsTransactionFeeDetails",
        "interchangeFeeAmount",
        "interchangeFeeCurrency",
        "interchangeFeeSign",
        "transactionFeeAmount",
        "ATMServiceFee",
        "terminalID",
        "terminalType",
        "transactionDescription",
        "additionalInformation",
        "MSpayerName",
        "MSpayerAddress",
        "MSpayerCity",
        "MSpayerState",
        "MSpayerCountryCode",
        "MSpayerPostalCode",
        "MSpayerDateOfBirth",
        "MSTraceReferenceNumber",
        "MSpayeeName",
        "MSpayeePhone",
        "additionalAmountDetails",
        "processingCode",
        "POSEntryMode",
        "cardSequenceNumber",
        "functionCode",
        "messageReasonCode",
        "transactionLifeCycleID",
        "matchingIndicator",
        "chipData",
        "onlineIndicator",
        "eCommerceSecurityLevelIndicator",
        "mastercardMappingServiceAccountNumber",
        "walletIdentifier",
        "dataRecord",
        "IPMMessageNumber",
        "IPMfileID"
    };

    final String[] fileFooterColumnNames = {
        "recordTypeID",
        "bankIdentifier",
        "fileSequentialNumber",
        "fileCreationDate",
        "recordsCounter"
    };
    final String[] bankOriginTransColumnNames = {
        "recordTypeID",
        "transactionDate",
        "transactionCode",
        "transactionTypeCode",
        "serviceClass",
        "requestCategory",
        "postingStatus",
        "postingDate",
        "responseCode",
        "recordID",
        "RRN",
        "authCode",
        "sourceContractID",
        "sourceContractNumber",
        "sourceContractCBSNumber",
        "sourceContractClientID",
        "sourceMainContractID",
        "sourceMainContractClientID",
        "sourceCardExpiryDate",
        "targetContractID",
        "targetContractNumber",
        "targetContractCBSNumber",
        "targetContractClientID",
        "targetMainContractID",
        "targetMainContractClientID",
        "targetCardExpiryDate",
        "targetContractPostingAmount",
        "targetContractPostingCurrency",
        "targetContractPostingSign",
        "sourceContractPostingAmount",
        "sourceContractPostingCurrency",
        "sourceContractPostingSign",
        "cmsTransactionFeeDetails",
        "transactionDescription",
        "transactionDescriptionProvidedByBank",
        "additionalInformation"
    };

    final String[] fileHeaderColumnNames = {
        "recordTypeID",
        "bankIdentifier",
        "fileSequentialNumber",
        "fileCreationDate"
    };

    private final Connection con;

// --Commented out by Inspection START (12/03/2024 10:43):
//    public MultiTableTSVProcessor() throws Exception {
//        this.con = DatabaseConnection.getConnection();
//    }
// --Commented out by Inspection STOP (12/03/2024 10:43)

    private String recordtypeConverter(String recordTypeId) {
        if (recordTypeId.equals("FH")) {
            return "fileheader";
        }
        if (recordTypeId.equals("FT")) {
            return "filefooter";
        }
        if (recordTypeId.equals("TX")) {
            return "transactionfromscheme";
        }
        if (recordTypeId.equals("BT")) {
            return "bankoriginatedtransaction";
        }
        if (recordTypeId.equals("MT")) {
            return "mporiginatedtransaction";
        }
        if (recordTypeId.equals("MF")) {
            return "miscellaneousfee";
        }
        if (recordTypeId.equals("SP")) {
            return "sepapayment";
        }
        if (recordTypeId.equals("IT")) {
            return "interest";
        }
        if (recordTypeId.equals("CB")) {
            return "chargeback";
        }
        if (recordTypeId.equals("FC")) {
            return "feecollections";
        }
        if (recordTypeId.equals("CT")) {
            return "clearingtotals";
        }
        return recordTypeId;

    }

    private void insertIntoDatabase(String tableName, String[] headers, String[] record, String[] tableFields)
            throws SQLException {
        StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(tableName).append(" (");

//        for (String header : headers) {
//            insertSQL.append(header).append(", ");
//        }
        for (int i = 0; i < tableFields.length; i++) {
            insertSQL.append(tableFields[i].toLowerCase());

            if (i < tableFields.length - 1) {
                insertSQL.append(" , ");
            }
        }

//        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(") VALUES (");

        int length = record.length;
        for (int i = 0; i < length; i++) {
            String field = record[i];
            if (field.isEmpty()) {
                insertSQL.append("null");
            } else {
                insertSQL.append("'").append(field).append("'");
            }
            if (i < length - 1) {
                insertSQL.append(", ");
            }
        }

//        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(")");
        System.out.println("SQL: " + insertSQL);

        try (PreparedStatement preparedStatement = con.prepareStatement(insertSQL.toString())) {
            preparedStatement.executeUpdate();
        }
    }

    public boolean storeRecordsInDatabase(String recordTypeId, String[] headers, List<String[]> records) {

        String[] tableFields = null;
        if (recordTypeId.equals("FH")) {
            tableFields = fileHeaderColumnNames;
        }
        if (recordTypeId.equals("FT")) {
            tableFields = fileFooterColumnNames;
        }
        if (recordTypeId.equals("TX")) {
            tableFields = transactionFromSchemeColumnNames;
        }
        if (recordTypeId.equals("BT")) {
            tableFields = bankOriginTransColumnNames;
        }
        if (recordTypeId.equals("MT")) {
            tableFields = mtColumnNames;
        }
        if (recordTypeId.equals("MF")) {
            tableFields = mfColumnNames;
        }
        if (recordTypeId.equals("SP")) {
            tableFields = spColumnNames;
        }
        if (recordTypeId.equals("IT")) {
            tableFields = itColumnNames;
        }
        if (recordTypeId.equals("CB")) {
            tableFields = cbColumnNames;
        }
        if (recordTypeId.equals("FC")) {
            tableFields = feeCollectionsColumns;
        }
        if (recordTypeId.equals("CT")) {
            tableFields = clearingTotalsColumns;
        }
        String tableName = recordtypeConverter(recordTypeId.toUpperCase()); //get full table name

        try {
            for (String[] record : records) {
                insertIntoDatabase(tableName, headers, record, tableFields);
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

//    private static void displayTables(Map<String, String[]> recordTypeToHeaders) {
//        // Display tables in tabular format
//        for (Map.Entry<String, String[]> entry : recordTypeToHeaders.entrySet()) {
//            System.out.println("Table: " + entry.getKey());
//            System.out.println("Headers: ");
//            for (String header : entry.getValue()) {
//                System.out.print(header + "\t");
//            }
//            System.out.println("\n");
//        }
//    }
//    public class CustomParser extends CSVParser {
//
//    public CustomParser(char separator) {
//        super(separator, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_IGNORE_LEADING_WHITESPACE,
//                DEFAULT_IGNORE_QUOTATIONS, true,DEFAULT_NULL_FIELD_INDICATOR, Locale.getDefault());
//    }
}
