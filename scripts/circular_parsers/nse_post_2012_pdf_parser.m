let
    // 1. Get all PDF files from a folder
    SourceFolder = Folder.Files(FolderPath),
    PDFsOnly = Table.SelectRows(SourceFolder, each Text.EndsWith([Extension], ".pdf")),

    // 2. Add a column to process each PDF
    ProcessedPDFs = Table.AddColumn(PDFsOnly, "HolidayData", each 
        let
            FileContent = Pdf.Tables([Content], [Implementation="1.3"]),
            #"Added Column Count" = Table.AddColumn(FileContent, "ColCount", each Table.ColumnCount([Data])),

            // Holiday tables
            Holidays = Table.SelectRows(#"Added Column Count", each [ColCount] = 4),
            #"Expanded Data" = Table.ExpandTableColumn(Holidays, "Data", {"Column1", "Column2", "Column3", "Column4"}, {"Column1", "Column2", "Column3", "Column4"}),
            #"Removed Other Columns" = Table.SelectColumns(#"Expanded Data",{"Column2", "Column4"}),
            #"Filtered Rows" = Table.SelectRows(#"Removed Other Columns", each not Text.Contains([Column2], "Date")),
            #"Renamed Columns" = Table.RenameColumns(#"Filtered Rows",{{"Column2", "date"}, {"Column4", "description"}}),
            #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","*","",Replacer.ReplaceText,{"description"}),
            #"Trimmed Text" = Table.TransformColumns(#"Replaced Value",{{"description", Text.Trim, type text}}),
            #"Changed Type" = Table.TransformColumnTypes(#"Trimmed Text",{{"date", type date}}),
            #"Add Holiday Type" = Table.AddColumn(#"Changed Type", "session_type", each "Trading Holiday", type text),

            // Circular table
            CircularTables = Table.SelectRows(#"Added Column Count", each 
            let
                firstRow = try Table.First([Data]) otherwise null
            in
            firstRow <> null and List.AnyTrue(List.Transform(Record.FieldValues(firstRow), each Text.Contains(_, "DEPARTMENT",  Comparer.OrdinalIgnoreCase)))),
            Table001 = CircularTables{0}[Data],

            // Convert all columns to text
            TableText = Table.TransformColumnTypes(
                Table001,
                List.Transform(Table.ColumnNames(Table001), each {_, type text})
            ),

            // Get the last column dynamically
            lastCol = List.Last(Table.ColumnNames(TableText)),
            #"Removed Other Columns1" = Table.SelectColumns(
                Table001,
                { lastCol }
            ),
            #"Filtered Rows1" = Table.SelectRows(#"Removed Other Columns1", each Text.Contains(Record.Field(_, lastCol), "Date")),
            #"Replaced Value1" = Table.ReplaceValue(
                #"Filtered Rows1",
                "Date : ",
                "",
                Replacer.ReplaceText,
                { lastCol }
            ),
            #"Changed Type2" = Table.TransformColumnTypes(
                #"Replaced Value1",
                {{ lastCol, type date }}
            ),

            // Add circular date
            #"Added Circular Date" = Table.AddColumn(
                #"Add Holiday Type", 
                "circular_date", 
                each #"Changed Type2"{0}{ lastCol }, 
                type date
            )
        in
            #"Added Circular Date"
    ),

    // 3. Expand all processed tables into one
    #"Expanded All PDFs" = Table.ExpandTableColumn(ProcessedPDFs, "HolidayData", {"date", "description", "session_type", "circular_date"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded All PDFs",{"date", "description", "session_type", "circular_date"})
in
    #"Removed Other Columns"