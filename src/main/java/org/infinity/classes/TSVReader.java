package org.infinity.classes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

public class TSVReader {
    public static void readAndStoreCSEData(String filepath) throws Exception{
        try (BufferedReader br = new BufferedReader(new FileReader(filepath));
             Connection connection = DatabaseConnection.getConnection()){
            String line;
            String currentRecordType = null;


            while ((line = br.readLine()) != null){
              String[] data  = line.split("\t");
                switch (data[0]) {
                    case "FH" -> {

                        if (data.length != 5) {
                            continue;
                        }

                        String recordTypeID = data[1];
                        String bankIdentifier = data[2];
                        int fileSequentialNumber = Integer.parseInt(data[3]);
                        String fileCreationDateStr = data[4];

                        Date fileCreationDate = null;
                        try {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
                            java.util.Date parsedDate = dateFormat.parse(fileCreationDateStr);
                            fileCreationDate = new Date(parsedDate.getTime());
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        String query = "INSERT INTO CSE_FH_Table (recordTypeID, bankIdentifier, fileSequentialNumber, fileCreationDate) VALUES (?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, bankIdentifier);
                            preparedStatement.setInt(3, fileSequentialNumber);
                            preparedStatement.setDate(4, fileCreationDate);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    case "FT" -> {
                        currentRecordType = "FT";

                        if (data.length != 5) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }

                        String recordTypeID = data[1];
                        String bankIdentifier = data[2];
                        int fileSequentialNumber = Integer.parseInt(data[3]);
                        String fileCreationDateStr = data[4];
                        int recordsCounter = Integer.parseInt(data[5]);

                        String query = "INSERT INTO CSE_FT_Table (recordTypeID, bankIdentifier, fileSequentialNumber, fileCreationDate, recordsCounter) VALUES (?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, bankIdentifier);
                            preparedStatement.setInt(3, fileSequentialNumber);
                            preparedStatement.setString(4, fileCreationDateStr);
                            preparedStatement.setInt(5, recordsCounter);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }


                    }
                    case "CL" -> {
                        if (data.length != 51) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }
                        String recordTypeID = data[1];
                        String clientID = data[2];
                        String clientNumber = data[3];
                        String orderDepartment = data[4];
                        String clientType = data[5];
                        String serviceGroup = data[6];
                        String identificationDocumentNumber = data[7];
                        String identificationDocumentType = data[8];
                        String identificationDocumentDetails = data[9];
                        String socialNumber = data[10];
                        String taxpayerIdentifier = data[11];
                        String taxPosition = data[12];
                        String shortName = data[13];
                        String title = data[14];
                        String firstName = data[15];
                        String lastName = data[16];
                        String middleName = data[17];
                        String secretPhase = data[18];
                        String suffix = data[19];
                        String countryCode = data[20];
                        String citizenship = data[21];
                        String language = data[22];
                        String maritalStatus = data[23];
                        String birthDate = data[24];
                        String birthPlace = data[25];
                        String birthName = data[27];
                        String gender = data[28];
                        String position = data[29];
                        String companyName = data[30];
                        String companyTradeName = data[31];
                        String companyDepartment = data[32];
                        String addressLine1 = data[33];
                        String addressLine2 = data[34];
                        String addressLine3 = data[35];
                        String addressLine4 = data[36];
                        String postalCode = data[37];
                        String city = data[38];
                        String state = data[39];
                        String email = data[40];
                        String phoneNumberMobile = data[41];
                        String phoneNumberWork = data[42];
                        String phoneNumberHome = data[43];
                        String fax = data[44];
                        String faxHome = data[45];
                        String embossedTitle = data[46];
                        String embossedFirstName = data[47];
                        String embossedLastName = data[48];
                        String embossedCompanyName = data[49];
                        String amendmentDate = data[50];
                        String amendmentOfficer = data[51];

                        String query = "INSERT INTO CSE_CL_Table (recordTypeID, clientID, clientNumber, orderDepartment, clientType, serviceGroup," +
                                " identificationDocumentNumber, identificationDocumentType, identificationDocumentDetails, socialNumber," +
                                " taxpayerIdentifier, taxPosition, shortName, title, firstName, lastName, middleName, secretPhrase, suffix," +
                                " countryCode, citizenship, language, maritalStatus, birthDate, birthPlace, birthName, gender," +
                                " position, companyName, companyTradeName, companyDepartment, addressLine1, addressLine2," +
                                " addressLine3, addressLine4, postalCode, city, state, email, phoneNumberMobile, phoneNumberWork," +
                                " phoneNumberHome, fax, faxHome, embossedTitle, embossedFirstName, embossedLastName, embossedCompanyName, amendmentDate," +
                                " amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, clientID);
                            preparedStatement.setString(3, clientNumber);
                            preparedStatement.setString(4, orderDepartment);
                            preparedStatement.setString(5, clientType);
                            preparedStatement.setString(6, serviceGroup);
                            preparedStatement.setString(7, identificationDocumentNumber);
                            preparedStatement.setString(8, identificationDocumentType);
                            preparedStatement.setString(9, identificationDocumentDetails);
                            preparedStatement.setString(10, socialNumber);
                            preparedStatement.setString(11, taxpayerIdentifier);
                            preparedStatement.setString(12, taxPosition);
                            preparedStatement.setString(13, shortName);
                            preparedStatement.setString(14, title);
                            preparedStatement.setString(15, firstName);
                            preparedStatement.setString(16, lastName);
                            preparedStatement.setString(17, middleName);
                            preparedStatement.setString(18, secretPhase);
                            preparedStatement.setString(19, suffix);
                            preparedStatement.setString(20, countryCode);
                            preparedStatement.setString(21, citizenship);
                            preparedStatement.setString(22, language);
                            preparedStatement.setString(23, maritalStatus);
                            preparedStatement.setString(24, birthDate);
                            preparedStatement.setString(25, birthPlace);
                            preparedStatement.setString(26, birthName);
                            preparedStatement.setString(27, gender);
                            preparedStatement.setString(28, position);
                            preparedStatement.setString(29, companyName);
                            preparedStatement.setString(30, companyTradeName);
                            preparedStatement.setString(31, companyDepartment);
                            preparedStatement.setString(32, addressLine1);
                            preparedStatement.setString(33, addressLine2);
                            preparedStatement.setString(34, addressLine3);
                            preparedStatement.setString(35, addressLine4);
                            preparedStatement.setString(36, postalCode);
                            preparedStatement.setString(37, city);
                            preparedStatement.setString(38, state);
                            preparedStatement.setString(39, email);
                            preparedStatement.setString(40, phoneNumberMobile);
                            preparedStatement.setString(41, phoneNumberWork);
                            preparedStatement.setString(42, phoneNumberHome);
                            preparedStatement.setString(43, fax);
                            preparedStatement.setString(44, faxHome);
                            preparedStatement.setString(45, embossedTitle);
                            preparedStatement.setString(46, embossedFirstName);
                            preparedStatement.setString(47, embossedLastName);
                            preparedStatement.setString(48, embossedCompanyName);
                            preparedStatement.setString(49, amendmentDate);
                            preparedStatement.setString(50, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    case "AC" -> {
                        if (data.length != 21) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }
                        String recordTypeID = data[1];
                        String accountContractID = data[2];
                        String accountContractNumber = data[3];
                        String accountCBSNumber = data[4];
                        String parentAccountContractID = data[5];
                        String parentAccountContractNumber = data[6];
                        String parentAccountCBSNumber = data[7];
                        String accountOwnerClientID = data[8];
                        String accountOwnerClientNumber = data[9];
                        String contractName = data[10];
                        Date dateOpen = Date.valueOf(data[11]);
                        Date dateExpiry = Date.valueOf(data[12]);
                        String currency = data[13];
                        String productCode = data[14];
                        String contractSubtypeCode = data[15];
                        String accountStatus = data[16];
                        String accountBalances = data[17];
                        String accountClassifiers = data[18];
                        Date amendmentDate = Date.valueOf(data[19]);
                        String amendmentOfficer = data[20];

                        String query = "INSERT INTO CSE_AC_Table (recordTypeID, accountContractID, accountContractNumber, accountCBSNumber, parentAccountContractID, parentAccountContractNumber, parentAccountCBSNumber, accountOwnerClientID, accountOwnerClientNumber, contractName, dateOpen, dateExpiry, currency, productCode, contractSubtypeCode, accountStatus, accountBalances, accountClassifiers, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, accountContractID);
                            preparedStatement.setString(3, accountContractNumber);
                            preparedStatement.setString(4, accountCBSNumber);
                            preparedStatement.setString(5, parentAccountContractID);
                            preparedStatement.setString(6, parentAccountContractNumber);
                            preparedStatement.setString(7, parentAccountCBSNumber);
                            preparedStatement.setString(8, accountOwnerClientID);
                            preparedStatement.setString(9, accountOwnerClientNumber);
                            preparedStatement.setString(10, contractName);
                            preparedStatement.setDate(11, dateOpen);
                            preparedStatement.setDate(12, dateExpiry);
                            preparedStatement.setString(13, currency);
                            preparedStatement.setString(14, productCode);
                            preparedStatement.setString(15, contractSubtypeCode);
                            preparedStatement.setString(16, accountStatus);
                            preparedStatement.setString(17, accountBalances);
                            preparedStatement.setString(18, accountClassifiers);
                            preparedStatement.setDate(19, amendmentDate);
                            preparedStatement.setString(20, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }


                    }
                    case "CR" -> {
                        currentRecordType = "CR";

                        if (data.length != 26) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }
                        // Extract data from the CR record
                        String recordTypeID = data[1];
                        String cardContractID = data[2];
                        String cardContractNumber = data[3];
                        String cardCBSNumber = data[4];
                        String accountContractID = data[5];
                        String accountContractNumber = data[6];
                        String accountCBSNumber = data[7];
                        String cardholderClientID = data[8];
                        String cardholderClientNumber = data[9];
                        String accountOwnerClientID = data[10];
                        String accountOwnerClientNumber = data[11];
                        String contractName = data[12];
                        Date dateOpen = Date.valueOf(data[13]);
                        String cardExpiryDate = data[14];
                        String currency = data[15];
                        String productCode = data[16];
                        String contractSubtypeCode = data[17];
                        String cardStatus = data[18];
                        String productionCode = data[19];
                        String embossedTitle = data[20];
                        String embossedFirstName = data[21];
                        String embossedLastName = data[22];
                        String embossedCompanyName = data[23];
                        Date amendmentDate = Date.valueOf(data[24]);
                        String amendmentOfficer = data[25];


                        String query = "INSERT INTO CSE_CR_Table (recordTypeID, cardContractID, cardContractNumber, cardCBSNumber, accountContractID, accountContractNumber, accountCBSNumber, cardholderClientID, cardholderClientNumber, accountOwnerClientID, accountOwnerClientNumber, contractName, dateOpen, cardExpiryDate, currency, productCode, contractSubtypeCode, cardStatus, productionCode, embossedTitle, embossedFirstName, embossedLastName, embossedCompanyName, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, cardContractID);
                            preparedStatement.setString(3, cardContractNumber);
                            preparedStatement.setString(4, cardCBSNumber);
                            preparedStatement.setString(5, accountContractID);
                            preparedStatement.setString(6, accountContractNumber);
                            preparedStatement.setString(7, accountCBSNumber);
                            preparedStatement.setString(8, cardholderClientID);
                            preparedStatement.setString(9, cardholderClientNumber);
                            preparedStatement.setString(10, accountOwnerClientID);
                            preparedStatement.setString(11, accountOwnerClientNumber);
                            preparedStatement.setString(12, contractName);
                            preparedStatement.setDate(13, dateOpen);
                            preparedStatement.setString(14, cardExpiryDate);
                            preparedStatement.setString(15, currency);
                            preparedStatement.setString(16, productCode);
                            preparedStatement.setString(17, contractSubtypeCode);
                            preparedStatement.setString(18, cardStatus);
                            preparedStatement.setString(19, productionCode);
                            preparedStatement.setString(20, embossedTitle);
                            preparedStatement.setString(21, embossedFirstName);
                            preparedStatement.setString(22, embossedLastName);
                            preparedStatement.setString(23, embossedCompanyName);
                            preparedStatement.setDate(24, amendmentDate);
                            preparedStatement.setString(25, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    case "CS" -> {
                        currentRecordType = "CS";

                        if (data.length != 10) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }

                        // Extract data from the CS record
                        String recordTypeID = data[1];
                        String cardContractID = data[2];
                        String cardContractNumber = data[3];
                        String cardCBSNumber = data[4];
                        String cardStatus = data[5];
                        String comment = data[6];
                        String statusChangeDate = data[7];
                        String statusChangeTime = data[8];
                        String amendmentDate = data[9];
                        String amendmentOfficer = data[10];

                        String query = "INSERT INTO CSE_CS_Table (recordTypeID, cardContractID, cardContractNumber, cardCBSNumber, cardStatus, comment, statusChangeDate, statusChangeTime, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, cardContractID);
                            preparedStatement.setString(3, cardContractNumber);
                            preparedStatement.setString(4, cardCBSNumber);
                            preparedStatement.setString(5, cardStatus);
                            preparedStatement.setString(6, comment);
                            preparedStatement.setString(7, statusChangeDate);
                            preparedStatement.setString(8, statusChangeTime);
                            preparedStatement.setString(9, amendmentDate);
                            preparedStatement.setString(10, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    case "AD" -> {
                        if (data.length != 19) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }

                        // Extract data from the AD record
                        String recordTypeID = data[1];
                        String contractID = data[2];
                        String contractNumber = data[3];
                        String contractCBSNumber = data[4];
                        String clientID = data[5];
                        String clientNumber = data[6];
                        String addressType = data[7];
                        String firstName = data[8];
                        String lastName = data[9];
                        String email = data[10];
                        String addressLine1 = data[11];
                        String addressLine2 = data[12];
                        String addressLine3 = data[13];
                        String addressLine4 = data[14];
                        String postalCode = data[15];
                        String city = data[16];
                        String state = data[17];
                        String country = data[18];
                        String status = data[19];

                        String query = "INSERT INTO CSE_AD_Table (recordTypeID, contractID, contractNumber, contractCBSNumber, clientID, clientNumber, addressType, firstName, lastName, email, addressLine1, addressLine2, addressLine3, addressLine4, postalCode, city, state, country, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, contractID);
                            preparedStatement.setString(3, contractNumber);
                            preparedStatement.setString(4, contractCBSNumber);
                            preparedStatement.setString(5, clientID);
                            preparedStatement.setString(6, clientNumber);
                            preparedStatement.setString(7, addressType);
                            preparedStatement.setString(8, firstName);
                            preparedStatement.setString(9, lastName);
                            preparedStatement.setString(10, email);
                            preparedStatement.setString(11, addressLine1);
                            preparedStatement.setString(12, addressLine2);
                            preparedStatement.setString(13, addressLine3);
                            preparedStatement.setString(14, addressLine4);
                            preparedStatement.setString(15, postalCode);
                            preparedStatement.setString(16, city);
                            preparedStatement.setString(17, state);
                            preparedStatement.setString(18, country);
                            preparedStatement.setString(19, status);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    case "LM" -> {

                        if (data.length != 13) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }

                        // Extract data from the LM record
                        String recordTypeID = data[1];
                        String contractID = data[2];
                        String contractNumber = data[3];
                        String contractCBSNumber = data[4];
                        String usageLimitCode = data[5];
                        int maxNumber = Integer.parseInt(data[6]);
                        double maxAmount = Double.parseDouble(data[7]);
                        double maxSingleAmount = Double.parseDouble(data[8]);
                        String currencyCode = data[9];
                        String activityPeriodDateFrom = data[10];
                        String activityPeriodDateTo = data[11];
                        int usedNumber = Integer.parseInt(data[12]);
                        double usedAmount = Double.parseDouble(data[13]);

                        String query = "INSERT INTO CSE_LM_Table (recordTypeID, contractID, contractNumber, contractCBSNumber, usageLimitCode, maxNumber, maxAmount, maxSingleAmount, currencyCode, activityPeriodDateFrom, activityPeriodDateTo, usedNumber, usedAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, contractID);
                            preparedStatement.setString(3, contractNumber);
                            preparedStatement.setString(4, contractCBSNumber);
                            preparedStatement.setString(5, usageLimitCode);
                            preparedStatement.setInt(6, maxNumber);
                            preparedStatement.setDouble(7, maxAmount);
                            preparedStatement.setDouble(8, maxSingleAmount);
                            preparedStatement.setString(9, currencyCode);
                            preparedStatement.setString(10, activityPeriodDateFrom);
                            preparedStatement.setString(11, activityPeriodDateTo);
                            preparedStatement.setInt(12, usedNumber);
                            preparedStatement.setDouble(13, usedAmount);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    default -> {
                        System.out.println("The record type is not supported");
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
