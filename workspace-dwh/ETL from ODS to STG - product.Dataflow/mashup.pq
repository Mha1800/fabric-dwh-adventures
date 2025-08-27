[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "stg product_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ProductSKU", DestinationColumnName = "ProductSKU"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"], [SourceColumnName = "ModelName", DestinationColumnName = "ModelName"], [SourceColumnName = "ProductDescription", DestinationColumnName = "ProductDescription"], [SourceColumnName = "ProductColor", DestinationColumnName = "ProductColor"], [SourceColumnName = "ProductSize", DestinationColumnName = "ProductSize"], [SourceColumnName = "ProductStyle", DestinationColumnName = "ProductStyle"], [SourceColumnName = "ProductId", DestinationColumnName = "ProductId"], [SourceColumnName = "ProductPrice", DestinationColumnName = "ProductPrice"], [SourceColumnName = "ProductCost", DestinationColumnName = "ProductCost"], [SourceColumnName = "ProductSubcategoryId", DestinationColumnName = "ProductSubcategoryId"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"stg product" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "product"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"ProductKey", "ProductId"}, {"ProductSubcategoryKey", "ProductSubcategoryId"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"ProductCost", type number}, {"ProductPrice", type number}, {"ProductSubcategoryId", Int64.Type}, {"ProductId", Int64.Type}})
in
  #"Changed column type";
shared #"stg product_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  TableNavigation = Navigation_2{[Schema = "stg", Item = "product"]}[Data]
in
  TableNavigation;
