package org.infinity.classes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

              List<String> data  = List.of(line.split("\t"));
                switch (data.get(0)) {
                    case "FH":

                        String recordTypeID = data.get(0);
                        String bankIdentifier = data.get(1);
                        String fileSequentialNumber = data.get(2);
                        String fileCreationDateStr = data.get(3);

                        String DropTable = "DROP TABLE IF EXISTS CSE_FH_Table";
                        connection.createStatement().execute(DropTable);



                        if (tableExists(connection, "CSE_FH_Table")){
                            String query = "CREATE TABLE CSE_FH_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), bankIdentifier VARCHAR(255), fileSequentialNumber VARCHAR(255), fileCreationDate VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_FH_Table' created successfully.");
                        }else{
                            System.out.println("Table 'ATRTable' already exists.");
                        }

                        String query = "INSERT INTO CSE_FH_Table (recordTypeID, bankIdentifier, fileSequentialNumber, fileCreationDate) VALUES (?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, bankIdentifier);
                            preparedStatement.setString(3, fileSequentialNumber);
                            preparedStatement.setString(4, fileCreationDateStr);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    case "FT":
                        recordTypeID = data.get(0);
                        bankIdentifier = data.get(1);
                        fileSequentialNumber = data.get(2);
                        fileCreationDateStr = data.get(3);
                        String recordsCounter = data.get(4);

                        DropTable = "DROP TABLE IF EXISTS CSE_FT_Table";
                        connection.createStatement().execute(DropTable);


                        if (tableExists(connection, "CSE_FT_Table")){
                            query = "CREATE TABLE CSE_FT_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), bankIdentifier VARCHAR(255), fileSequentialNumber VARCHAR(255), fileCreationDate VARCHAR(255), recordsCounter VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_FT_Table' created successfully.");

                        }

                        query = "INSERT INTO CSE_FT_Table (recordTypeID, bankIdentifier, fileSequentialNumber, fileCreationDate, recordsCounter) VALUES (?, ?, ?, ?, ?)";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, bankIdentifier);
                            preparedStatement.setString(3, fileSequentialNumber);
                            preparedStatement.setString(4, fileCreationDateStr);
                            preparedStatement.setString(5, recordsCounter);
                            preparedStatement.executeUpdate();
                            System.out.println("Data inserted successfully");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }



                case "CL" :
                        if (data.size() != 60) {
                            // Handle the error, skip the line, or throw an exception
                            continue;
                        }
                        recordTypeID = data.get(0);
                        String clientID = data.get(1);
                        String clientNumber = data.get(2);
                        String orderDepartment = data.get(3);
                        String clientType = data.get(4);
                        String serviceGroup = data.get(5);
                        String identificationDocumentNumber = data.get(6);
                        String identificationDocumentType = data.get(7);
                        String identificationDocumentDetails = data.get(8);
                        String socialNumber = data.get(9);
                        String taxpayerIdentifier = data.get(10);
                        String taxPosition = data.get(11);
                        String shortName = data.get(12);
                        String title = data.get(13);
                        String firstName = data.get(14);
                        String lastName = data.get(15);
                        String middleName = data.get(16);
                        String secretPhase = data.get(17);
                        String suffix = data.get(18);
                        String countryCode = data.get(19);
                        String citizenship = data.get(20);
                        String language = data.get(21);
                        String maritalStatus = data.get(22);
                        String birthDate = data.get(23);
                        String birthPlace = data.get(24);
                        String birthName = data.get(25);
                        String gender = data.get(26);
                        String position = data.get(27);
                        String companyName = data.get(28);
                        String companyTradeName = data.get(29);
                        String companyDepartment = data.get(30);
                        String addressLine1 = data.get(31);
                        String addressLine2 = data.get(32);
                        String addressLine3 = data.get(33);
                        String addressLine4 = data.get(34);
                        String postalCode = data.get(35);
                        String city = data.get(36);
                        String state = data.get(37);
                        String email = data.get(38);
                        String phoneNumberMobile = data.get(39);
                        String phoneNumberWork = data.get(40);
                        String phoneNumberHome = data.get(41);
                        String fax = data.get(42);
                        String faxHome = data.get(43);
                        String embossedTitle = data.get(44);
                        String embossedFirstName = data.get(45);
                        String embossedLastName = data.get(46);
                        String embossedCompanyName = data.get(47);
                        String amendmentDate = data.get(48);
                        String amendmentOfficer = data.get(49);

                        DropTable = "DROP TABLE IF EXISTS CSE_CL_Table";
                        connection.createStatement().execute(DropTable);


                        if (tableExists(connection, "CSE_CL_Table")){
                            query = "CREATE TABLE IF NOT EXISTS CSE_CL_Table (" +
                                    "recordTypeID VARCHAR(2) NOT NULL," +
                                    "clientID VARCHAR(32)," +
                                    "clientNumber VARCHAR(32) NOT NULL," +
                                    "orderDepartment VARCHAR(32)," +
                                    "clientType VARCHAR(5)," +
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
                                    "countryCode VARCHAR(3)," +
                                    "citizenship VARCHAR(3)," +
                                    "language VARCHAR(3)," +
                                    "maritalStatus VARCHAR(3)," +
                                    "birthDate VARCHAR(255)," +
                                    "birthPlace VARCHAR(255)," +
                                    "birthName VARCHAR(255)," +
                                    "gender ENUM('M', 'F', 'N')," +
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
                                    "phoneNumberWork VARCHAR(32)," +
                                    "phoneNumberHome VARCHAR(32)," +
                                    "fax VARCHAR(32)," +
                                    "faxHome VARCHAR(32)," +
                                    "embossedTitle VARCHAR(26)," +
                                    "embossedFirstName VARCHAR(255)," +
                                    "embossedLastName VARCHAR(255)," +
                                    "embossedCompanyName VARCHAR(26)," +
                                    "amendmentDate TIMESTAMP," +
                                    "amendmentOfficer VARCHAR(255)," +
                                    "PRIMARY KEY (clientNumber)" +
                                    ")";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_CL_Table' created successfully.");

                        }else{
                            System.out.println("Table 'ATRTable' already exists.");
                        }

                        query = "INSERT INTO CSE_CL_Table (recordTypeID, clientID, clientNumber, orderDepartment, clientType, serviceGroup," +
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
                case "AC":
                        recordTypeID = data.get(0);
                        String accountContractID = data.get(1);
                        String accountContractNumber = data.get(2);
                        String accountCBSNumber = data.get(3);
                        String parentAccountContractID = data.get(4);
                        String parentAccountContractNumber = data.get(5);
                        String parentAccountCBSNumber = data.get(6);
                        String accountOwnerClientID = data.get(7);
                        String accountOwnerClientNumber = data.get(8);
                        String contractName = data.get(9);
                        String dateOpen = data.get(10);
                        String dateExpiry = data.get(11);
                        String currency = data.get(12);
                        String productCode = data.get(13);
                        String contractSubtypeCode = data.get(14);
                        String accountStatus = data.get(15);
                        String accountBalances = data.get(16);
                        String accountClassifiers = data.get(17);
                        amendmentDate = data.get(19);
                        amendmentOfficer = data.get(19);

                        DropTable = "DROP TABLE IF EXISTS CSE_AC_Table";
                        connection.createStatement().execute(DropTable);

                        if (tableExists(connection, "CSE_AC_Table")) {
                            query = "CREATE TABLE CSE_AC_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), accountContractID VARCHAR(255), accountContractNumber VARCHAR(255), accountCBSNumber VARCHAR(255), parentAccountContractID VARCHAR(255), parentAccountContractNumber VARCHAR(255), parentAccountCBSNumber VARCHAR(255), accountOwnerClientID VARCHAR(255), accountOwnerClientNumber VARCHAR(255), contractName VARCHAR(50), dateOpen VARCHAR(255), dateExpiry VARCHAR(255), currency VARCHAR(255), productCode VARCHAR(255), contractSubtypeCode VARCHAR(255), accountStatus VARCHAR(255), accountBalances VARCHAR(255), accountClassifiers VARCHAR(255), amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_AC_Table' created successfully.");
                        }else{
                            System.out.println("Table 'ATRTable' already exists.");
                        }

                        query = "INSERT INTO CSE_AC_Table (recordTypeID, accountContractID, accountContractNumber, accountCBSNumber, parentAccountContractID, parentAccountContractNumber, parentAccountCBSNumber, accountOwnerClientID, accountOwnerClientNumber, contractName, dateOpen, dateExpiry, currency, productCode, contractSubtypeCode, accountStatus, accountBalances, accountClassifiers, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                            preparedStatement.setString(11, dateOpen);
                            preparedStatement.setString(12, dateExpiry);
                            preparedStatement.setString(13, currency);
                            preparedStatement.setString(14, productCode);
                            preparedStatement.setString(15, contractSubtypeCode);
                            preparedStatement.setString(16, accountStatus);
                            preparedStatement.setString(17, accountBalances);
                            preparedStatement.setString(18, accountClassifiers);
                            preparedStatement.setString(19, amendmentDate);
                            preparedStatement.setString(20, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }



                case "CR":

//                        if (data.length != 26) {
//                            // Handle the error, skip the line, or throw an exception
//                            continue;
//                        }
                        // Extract data from the CR record
                        recordTypeID = data.get(0);
                        String cardContractID = data.get(1);
                        String cardContractNumber = data.get(2);
                        String cardCBSNumber = data.get(3);
                        accountContractID = data.get(4);
                        accountContractNumber = data.get(5);
                        accountCBSNumber = data.get(6);
                        String cardholderClientID = data.get(7);
                        String cardholderClientNumber = data.get(8);
                        accountOwnerClientID = data.get(9);
                        accountOwnerClientNumber = data.get(10);
                        contractName = data.get(11);
                        dateOpen = data.get(12);
                        String cardExpiryDate = data.get(13);
                        currency = data.get(14);
                        productCode = data.get(15);
                        contractSubtypeCode = data.get(16);
                        String cardStatus = data.get(17);
                        String productionCode = data.get(18);
                        embossedTitle = data.get(19);
                        embossedFirstName = data.get(20);
                        embossedLastName = data.get(21);
                        embossedCompanyName = data.get(22);
                        amendmentDate = data.get(23);
                        amendmentOfficer = data.get(24);

                        DropTable = "DROP TABLE IF EXISTS CSE_CR_Table";
                        connection.createStatement().execute(DropTable);

                        if (tableExists(connection, "CSE_CR_Table")){
                            query = "CREATE TABLE CSE_CR_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), cardContractID VARCHAR(255), cardContractNumber VARCHAR(255), cardCBSNumber VARCHAR(255), accountContractID VARCHAR(255), accountContractNumber VARCHAR(255), accountCBSNumber VARCHAR(255), cardholderClientID VARCHAR(255), cardholderClientNumber VARCHAR(255), accountOwnerClientID VARCHAR(255), accountOwnerClientNumber VARCHAR(255), contractName VARCHAR(255), dateOpen VARCHAR(255), cardExpiryDate VARCHAR(255), currency VARCHAR(255), productCode VARCHAR(255), contractSubtypeCode VARCHAR(255), cardStatus VARCHAR(255), productionCode VARCHAR(255), embossedTitle VARCHAR(255), embossedFirstName VARCHAR(255), embossedLastName VARCHAR(255), embossedCompanyName VARCHAR(255), amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_CR_Table' created successfully.");
                        }else{
                            System.out.println("Table 'ATRTable' already exists.");
                        }


                        query = "INSERT INTO CSE_CR_Table (recordTypeID, cardContractID, cardContractNumber, cardCBSNumber, accountContractID, accountContractNumber, accountCBSNumber, cardholderClientID, cardholderClientNumber, accountOwnerClientID, accountOwnerClientNumber, contractName, dateOpen, cardExpiryDate, currency, productCode, contractSubtypeCode, cardStatus, productionCode, embossedTitle, embossedFirstName, embossedLastName, embossedCompanyName, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                            preparedStatement.setString(13, dateOpen);
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
                            preparedStatement.setString(24, amendmentDate);
                            preparedStatement.setString(25, amendmentOfficer);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                case "CS":

                        // Extract data from the CS record
                        recordTypeID = data.get(0);
                        cardContractID = data.get(1);
                        cardContractNumber = data.get(2);
                        cardCBSNumber = data.get(3);
                        cardStatus = data.get(4);
                        String comment = data.get(5);
                        String statusChangeDate = data.get(6);
                        String statusChangeTime = data.get(7);
                        amendmentDate = data.get(8);
                        amendmentOfficer = data.get(9);

                        DropTable = "DROP TABLE IF EXISTS CSE_CS_Table";
                        connection.createStatement().execute(DropTable);


                        if (tableExists(connection, "CSE_CS_Table")){
                            query = "CREATE TABLE CSE_CS_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), cardContractID VARCHAR(255), cardContractNumber VARCHAR(255), cardCBSNumber VARCHAR(255), cardStatus VARCHAR(255), comment VARCHAR(255), statusChangeDate VARCHAR(255), statusChangeTime TIME, amendmentDate VARCHAR(255), amendmentOfficer VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_CS_Table' created successfully.");
                        }

                        query = "INSERT INTO CSE_CS_Table (recordTypeID, cardContractID, cardContractNumber, cardCBSNumber, cardStatus, comment, statusChangeDate, statusChangeTime, amendmentDate, amendmentOfficer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                case "AD":

                        recordTypeID = data.get(0);
                        String contractID = data.get(1);
                        String contractNumber = data.get(2);
                        String contractCBSNumber = data.get(3);
                        clientID = data.get(4);
                        clientNumber = data.get(5);
                        String addressType = data.get(6);
                        firstName = data.get(7);
                        lastName = data.get(8);
                        email = data.get(9);
                        addressLine1 = data.get(10);
                        addressLine2 = data.get(11);
                        addressLine3 = data.get(12);
                        addressLine4 = data.get(13);
                        postalCode = data.get(14);
                        city = data.get(15);
                        state = data.get(16);
                        String country = data.get(17);
                        String status = data.get(18);

                        DropTable = "DROP TABLE IF EXISTS CSE_AD_Table";
                        connection.createStatement().execute(DropTable);



                        if (tableExists(connection, "CSE_AD_Table")){
                            query = "CREATE TABLE CSE_AD_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), contractID VARCHAR(255), contractNumber VARCHAR(255), contractCBSNumber VARCHAR(255), clientID VARCHAR(255), clientNumber VARCHAR(255), addressType VARCHAR(255), firstName VARCHAR(255), lastName VARCHAR(255), email VARCHAR(20), addressLine1 VARCHAR(20), addressLine2 VARCHAR(20), addressLine3 VARCHAR(20), addressLine4 VARCHAR(20), postalCode VARCHAR(20), city VARCHAR(20), state VARCHAR(20), country VARCHAR(20), status VARCHAR(20))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_AD_Table' created successfully.");
                        }

                        query = "INSERT INTO CSE_AD_Table (recordTypeID, contractID, contractNumber, contractCBSNumber, clientID, clientNumber, addressType, firstName, lastName, email, addressLine1, addressLine2, addressLine3, addressLine4, postalCode, city, state, country, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                    case "LM" :


                        // Extract data from the LM record
                        recordTypeID = data.get(0);
                        contractID = data.get(1);
                        contractNumber = data.get(2);
                        contractCBSNumber = data.get(3);
                        String usageLimitCode = data.get(4);
                        String maxNumber = data.get(5);
                        String maxAmount = data.get(6);
                        String maxSingleAmount = data.get(7);
                        String currencyCode = data.get(8);
                        String activityPeriodDateFrom = data.get(9);
                        String activityPeriodDateTo = data.get(10);
                        String usedNumber = data.get(11);
                        String usedAmount = data.get(12);

                        DropTable = "DROP TABLE IF EXISTS CSE_LM_Table";
                        connection.createStatement().execute(DropTable);



                        if (tableExists(connection, "CSE_LM_Table")){
                            query = "CREATE TABLE CSE_LM_Table (id SERIAL PRIMARY KEY, recordTypeID VARCHAR(255), contractID VARCHAR(255), contractNumber VARCHAR(255), contractCBSNumber VARCHAR(255), usageLimitCode VARCHAR(255), maxNumber VARCHAR(255), maxAmount VARCHAR(255), maxSingleAmount VARCHAR(255), currencyCode VARCHAR(3), activityPeriodDateFrom VARCHAR(255), activityPeriodDateTo VARCHAR(255), usedNumber VARCHAR(255), usedAmount VARCHAR(255))";
                            connection.createStatement().execute(query);
                            System.out.println("Table 'CSE_LM_Table' created successfully.");
                        }

                        query = "INSERT INTO CSE_LM_Table (recordTypeID, contractID, contractNumber, contractCBSNumber, usageLimitCode, maxNumber, maxAmount, maxSingleAmount, currencyCode, activityPeriodDateFrom, activityPeriodDateTo, usedNumber, usedAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                            preparedStatement.setString(1, recordTypeID);
                            preparedStatement.setString(2, contractID);
                            preparedStatement.setString(3, contractNumber);
                            preparedStatement.setString(4, contractCBSNumber);
                            preparedStatement.setString(5, usageLimitCode);
                            preparedStatement.setString(6, maxNumber);
                            preparedStatement.setString(7, maxAmount);
                            preparedStatement.setString(8, maxSingleAmount);
                            preparedStatement.setString(9, currencyCode);
                            preparedStatement.setString(10, activityPeriodDateFrom);
                            preparedStatement.setString(11, activityPeriodDateTo);
                            preparedStatement.setString(12, usedNumber);
                            preparedStatement.setString(13, usedAmount);
                            preparedStatement.executeUpdate();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    default: System.out.println("The record type is not supported");
                }
            }
            System.out.println("Data inserted successfully");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
