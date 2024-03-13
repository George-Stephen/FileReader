package org.infinity;

import org.infinity.classes.TSVReader;

public class Main {
    public static void main(String[] args) {
        // Specify the path to your TSV file
        String TsvFilePath = "C:\\Users\\georg\\Desktop\\FileReader\\src\\main\\java\\org\\infinity\\resources\\CSE_CPO_20240309_001.tsv";

        // Specify the path to your CSV file
        String CsvFilePath = "C:\\Users\\georg\\Downloads\\ATR_CPO_20240227_001.csv\\ATR_CPO_20240227_001.csv";

        TSVReader.readAndStoreCSEData(TsvFilePath);
//        CSVReader.readAndStoreATRData(CsvFilePath);

    }
}