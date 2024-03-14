package org.infinity.classes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.infinity.classes.DatabaseConnection.tableExists;

public class TSVReader {
    public static void readAndStoreCSEData(String filepath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath));
             Connection connection = DatabaseConnection.getConnection()){
            String line;
            boolean isFirstLine = true; // Flag to ignore the first line

            while ((line = br.readLine()) != null){
                if (isFirstLine) {
                    isFirstLine = false;
                    continue; // Skip processing the first line
                }

//                List<String> data  = List.of(line.split("\t"));
                String[] array = line.split("\t");
                List<String> data = new ArrayList<>(Arrays.asList(array));
                  System.out.println(data.get(0));
                  switch (data.get(0)) {
                        case "FH":
                            String[] RecordNames = {
                                    "recordTypeID",
                                    "bankIdentifier",
                                    "fileSequentialNumber",
                                    "fileCreationDateStr",
                                    "recordsCounter"
                            };

                            if (tableExists(connection, "CSE_FH_Table")){
                                String query = "CREATE TABLE CSE_FH_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), bankIdentifier VARCHAR(255), fileSequentialNumber VARCHAR(255), fileCreationDate VARCHAR(255))";
                                connection.createStatement().execute(query);
                                System.out.println("Table 'CSE_FH_Table' created successfully.");
                            }else{
                                System.out.println("Table 'CSE_FH_Table' already exists.");
                            }

                            StringBuilder placeholders = new StringBuilder();
                            StringBuilder Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }

                            String query;
                            try {
                                query = "INSERT INTO CSE_FH_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                        case "FT":
                            RecordNames = new String[]{
                                    "recordTypeID",
                                    "bankIdentifier",
                                    "fileSequentialNumber",
                                    "fileCreationDateStr",
                                    "recordsCounter"
                            };
                            if (tableExists(connection, "CSE_FT_Table")){
                                query = "CREATE TABLE CSE_FT_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), bankIdentifier VARCHAR(255), fileSequentialNumber VARCHAR(255), fileCreationDate VARCHAR(255), recordsCounter VARCHAR(255))";
                                connection.createStatement().execute(query);
                                System.out.println("Table 'CSE_FT_Table' created successfully.");

                            }

                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }

                            try {
                                query = "INSERT INTO CSE_FT_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                      case "CL":
                            if (tableExists(connection, "CSE_CL_Table")){
                                query = "CREATE TABLE IF NOT EXISTS CSE_CL_Table (" +
                                        "recordTypeID VARCHAR(2) NOT NULL," +
                                        "clientID VARCHAR(32)," +
                                        "clientNumber VARCHAR(32) NOT NULL," +
                                        "orderDepartment VARCHAR(32)," +
                                        "clientType VARCHAR(255)," +
                                        "serviceGroup VARCHAR(32)," +
                                        "identificationDocumentNumber VARCHAR(32)," +
                                        "identificationDocumentType VARCHAR(32)," +
                                        "identificationDocumentDetails VARCHAR(255)," +
                                        "socialNumber VARCHAR(32)," +
                                        "taxpayerIdentifier VARCHAR(32)," +
                                        "taxPosition VARCHAR(32)," +
                                        "shortName VARCHAR(255)," +
                                        "title VARCHAR(32)," +
                                        "firstName VARCHAR(255)," +
                                        "lastName VARCHAR(255)," +
                                        "middleName VARCHAR(255)," +
                                        "secretPhrase VARCHAR(255)," +
                                        "suffix VARCHAR(32)," +
                                        "countryCode VARCHAR(255)," +
                                        "citizenship VARCHAR(255)," +
                                        "language VARCHAR(255)," +
                                        "maritalStatus VARCHAR(255)," +
                                        "birthDate VARCHAR(255)," +
                                        "birthPlace VARCHAR(255)," +
                                        "birthName VARCHAR(255)," +
                                        "gender VARCHAR(255)," +
                                        "position VARCHAR(32)," +
                                        "companyName VARCHAR(255)," +
                                        "companyTradeName VARCHAR(255)," +
                                        "companyDepartment VARCHAR(255)," +
                                        "addressLine1 VARCHAR(255)," +
                                        "addressLine2 VARCHAR(255)," +
                                        "addressLine3 VARCHAR(255)," +
                                        "addressLine4 VARCHAR(255)," +
                                        "postalCode VARCHAR(32)," +
                                        "city VARCHAR(255)," +
                                        "state VARCHAR(32)," +
                                        "email VARCHAR(255)," +
                                        "phoneNumberMobile VARCHAR(32)," +
                                        "phoneNumberWork VARCHAR(255)," +
                                        "phoneNumberHome VARCHAR(255)," +
                                        "fax VARCHAR(255)," +
                                        "faxHome VARCHAR(255)," +
                                        "embossedTitle VARCHAR(255)," +
                                        "embossedFirstName VARCHAR(255)," +
                                        "embossedLastName VARCHAR(255)," +
                                        "embossedCompanyName VARCHAR(255)," +
                                        "amendmentDate VARCHAR(255)" +
                                        ")";
                                connection.createStatement().execute(query);
                                System.out.println("Table 'CSE_CL_Table' created successfully.");

                            }
                          RecordNames = new String[]{
                                  "recordTypeID", "clientID", "clientNumber", "orderDepartment", "clientType", "serviceGroup",
                                  "identificationDocumentNumber", "identificationDocumentType", "identificationDocumentDetails",
                                  "socialNumber", "taxpayerIdentifier", "taxPosition", "shortName", "title", "firstName", "lastName",
                                  "middleName", "secretPhrase", "suffix", "countryCode", "citizenship", "language", "maritalStatus",
                                  "birthDate", "birthPlace", "birthName", "gender", "position", "companyName", "companyTradeName",
                                  "companyDepartment", "addressLine1", "addressLine2", "addressLine3", "addressLine4", "postalCode",
                                  "city", "state", "email", "phoneNumberMobile", "phoneNumberWork", "phoneNumberHome", "fax", "faxHome",
                                  "embossedTitle", "embossedFirstName", "embossedLastName", "embossedCompanyName", "amendmentDate",
                                  "amendmentOfficer"
                          };

                              placeholders = new StringBuilder();
                              Records = new StringBuilder();
                              for (int i = 0; i < data.size(); i++) {
                                  Records.append(RecordNames[i]);
                                  placeholders.append("?");
                                  if (i < data.size() - 1) {
                                      Records.append(",");
                                      placeholders.append(",");
                                  }
                              }

                            try {
                                query = "INSERT INTO CSE_CL_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                          break;
                    case "AC":
                        RecordNames = new String[]{
                                "recordTypeID",
                                "accountContractID",
                                "accountContractNumber",
                                "accountCBSNumber",
                                "parentAccountContractID",
                                "parentAccountContractNumber",
                                "parentAccountCBSNumber",
                                "accountOwnerClientID",
                                "accountOwnerClientNumber",
                                "contractName",
                                "dateOpen",
                                "dateExpiry",
                                "currency",
                                "productCode",
                                "contractSubtypeCode",
                                "accountStatus",
                                "accountBalances",
                                "accountClassifiers",
                                "amendmentDate",
                                "amendmentOfficer"
                        };

//                            if (tableExists(connection, "CSE_AC_Table")) {
//                                query = "CREATE TABLE CSE_AC_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), accountContractID VARCHAR(255), accountContractNumber VARCHAR(255), accountCBSNumber VARCHAR(255), parentAccountContractID VARCHAR(255), parentAccountContractNumber VARCHAR(255), parentAccountCBSNumber VARCHAR(255), accountOwnerClientID VARCHAR(255), accountOwnerClientNumber VARCHAR(255), contractName VARCHAR(50), dateOpen VARCHAR(255), dateExpiry VARCHAR(255), currency VARCHAR(255), productCode VARCHAR(255), contractSubtypeCode VARCHAR(255), accountStatus VARCHAR(255), accountBalances VARCHAR(255), accountClassifiers VARCHAR(255), amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
//                                connection.createStatement().execute(query);
//                                System.out.println("Table 'CSE_AC_Table' created successfully.");
//                            }else{
//                                System.out.println("Table already exists");
//                            }
                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                        for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }
                            try{
                                query = "INSERT INTO CSE_AC_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();

                            } catch (Exception e) {
                                e.printStackTrace();
                                break;
                            }
                            break;
                    case "CR":
                        RecordNames = new String[]{
                                "recordTypeID",
                                "cardContractID",
                                "cardContractNumber",
                                "cardCBSNumber",
                                "accountContractID",
                                "accountContractNumber",
                                "accountCBSNumber",
                                "cardholderClientID",
                                "cardholderClientNumber",
                                "accountOwnerClientID",
                                "accountOwnerClientNumber",
                                "contractName",
                                "dateOpen",
                                "cardExpiryDate",
                                "currency",
                                "productCode",
                                "contractSubtypeCode",
                                "cardStatus",
                                "productionCode",
                                "embossedTitle",
                                "embossedFirstName",
                                "embossedLastName",
                                "embossedCompanyName",
                                "amendmentDate",
                                "amendmentOfficer"
                        };

//                            if (tableExists(connection, "CSE_CR_Table")){
//                                query = "CREATE TABLE CSE_CR_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), cardContractID VARCHAR(255), cardContractNumber VARCHAR(255), cardCBSNumber VARCHAR(255), accountContractID VARCHAR(255), accountContractNumber VARCHAR(255), accountCBSNumber VARCHAR(255), cardholderClientID VARCHAR(255), cardholderClientNumber VARCHAR(255), accountOwnerClientID VARCHAR(255), accountOwnerClientNumber VARCHAR(255), contractName VARCHAR(255), dateOpen VARCHAR(255), cardExpiryDate VARCHAR(255), currency VARCHAR(255), productCode VARCHAR(255), contractSubtypeCode VARCHAR(255), cardStatus VARCHAR(255), productionCode VARCHAR(255), embossedTitle VARCHAR(255), embossedFirstName VARCHAR(255), embossedLastName VARCHAR(255), embossedCompanyName VARCHAR(255), amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
//                                connection.createStatement().execute(query);
//                                System.out.println("Table 'CSE_CR_Table' created successfully.");
//                            }else{
//                                System.out.println("Table 'CSE_CR_Table' already exists.");
//                            }

                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }
                            try{
                                query = "INSERT INTO CSE_CR_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        break;

                    case "CS":
                        RecordNames = new String[]{
                                "recordTypeID",
                                "cardContractID",
                                "cardContractNumber",
                                "cardCBSNumber",
                                "cardStatus",
                                "comment",
                                "statusChangeDate",
                                "statusChangeTime",
                                "amendmentDate",
                                "amendmentOfficer"
                        };


//                            if (tableExists(connection, "CSE_CS_Table")){
//                                query = "CREATE TABLE CSE_CS_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), cardContractID VARCHAR(255), cardContractNumber VARCHAR(255), cardCBSNumber VARCHAR(255), cardStatus VARCHAR(255), comment VARCHAR(255), statusChangeDate VARCHAR(255), statusChangeTime VARCHAR(255), amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
//                                connection.createStatement().execute(query);
//                                System.out.println("Table 'CSE_CS_Table' created successfully.");
//                            }

                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }
                            try{
                                query = "INSERT INTO CSE_CS_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                    case "AD":


                        RecordNames = new String[]{
                                "recordTypeID",
                                "contractID",
                                "contractNumber",
                                "contractCBSNumber",
                                "clientID",
                                "clientNumber",
                                "addressType",
                                "firstName",
                                "lastName",
                                "email",
                                "addressLine1",
                                "addressLine2",
                                "addressLine3",
                                "addressLine4",
                                "postalCode",
                                "city",
                                "state",
                                "country",
                                "status"
                        };


                            if (tableExists(connection, "CSE_AD_Table")){
                                query = "CREATE TABLE CSE_AD_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), contractID VARCHAR(255), contractNumber VARCHAR(255), contractCBSNumber VARCHAR(255), clientID VARCHAR(255), clientNumber VARCHAR(255), addressType VARCHAR(255), firstName VARCHAR(255), lastName VARCHAR(255), email VARCHAR(20), addressLine1 VARCHAR(20), addressLine2 VARCHAR(20), addressLine3 VARCHAR(20), addressLine4 VARCHAR(20), postalCode VARCHAR(20), city VARCHAR(20), state VARCHAR(20), country VARCHAR(20), status VARCHAR(20))";
                                connection.createStatement().execute(query);
                                System.out.println("Table 'CSE_AD_Table' created successfully.");
                            }
                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }
                            try{
                                query = "INSERT INTO CSE_AD_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            break;
                        case "LM" :

                            RecordNames = new String[]{
                                    "recordTypeID",
                                    "contractID",
                                    "contractNumber",
                                    "contractCBSNumber",
                                    "usageLimitCode",
                                    "maxNumber",
                                    "maxAmount",
                                    "maxSingleAmount",
                                    "currencyCode",
                                    "activityPeriodDateFrom",
                                    "activityPeriodDateTo",
                                    "usedNumber",
                                    "usedAmount"
                            };
//                            if (tableExists(connection, "CSE_LM_Table")){
//                                query = "CREATE TABLE CSE_LM_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), contractID VARCHAR(255), contractNumber VARCHAR(255), contractCBSNumber VARCHAR(255), usageLimitCode VARCHAR(255), maxNumber VARCHAR(255), maxAmount VARCHAR(255), maxSingleAmount VARCHAR(255), currencyCode VARCHAR(3), activityPeriodDateFrom VARCHAR(255), activityPeriodDateTo VARCHAR(255), usedNumber VARCHAR(255), usedAmount VARCHAR(255))";
//                                connection.createStatement().execute(query);
//                                System.out.println("Table 'CSE_LM_Table' created successfully.");
//                            }

                            placeholders = new StringBuilder();
                            Records = new StringBuilder();
                            for (int i = 0; i < data.size(); i++) {
                                Records.append(RecordNames[i]);
                                placeholders.append("?");
                                if (i < data.size() - 1) {
                                    Records.append(",");
                                    placeholders.append(", ");
                                }
                            }
                            try{
                                query = "INSERT INTO CSE_LM_Table ("+ Records + ") VALUES (" + placeholders + ")";
                                PreparedStatement preparedStatement = connection.prepareStatement(query);
                                for (int i = 0; i < data.size(); i++) {
                                    preparedStatement.setString(i+1, data.get(i));
                                }
                                preparedStatement.executeUpdate();

                            } catch (Exception e) {
                                e.printStackTrace();

                            }
                            break;
                        default: System.out.println("The record type is not supported");
                    }
            }
            System.out.println("Data inserted successfully");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
