[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "stg product_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ProductSKU", DestinationColumnName = "ProductSKU"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"], [SourceColumnName = "ModelName", DestinationColumnName = "ModelName"], [SourceColumnName = "ProductDescription", DestinationColumnName = "ProductDescription"], [SourceColumnName = "ProductColor", DestinationColumnName = "ProductColor"], [SourceColumnName = "ProductSize", DestinationColumnName = "ProductSize"], [SourceColumnName = "ProductStyle", DestinationColumnName = "ProductStyle"], [SourceColumnName = "ProductId", DestinationColumnName = "ProductId"], [SourceColumnName = "ProductPrice", DestinationColumnName = "ProductPrice"], [SourceColumnName = "ProductCost", DestinationColumnName = "ProductCost"], [SourceColumnName = "SubcategoryName", DestinationColumnName = "SubcategoryName"], [SourceColumnName = "CategoryName", DestinationColumnName = "CategoryName"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"stg product" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "product"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"ProductKey", "ProductId"}, {"ProductSubcategoryKey", "ProductSubcategoryId"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"ProductCost", type number}, {"ProductPrice", type number}, {"ProductSubcategoryId", Int64.Type}, {"ProductId", Int64.Type}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"ProductSubcategoryId"}, #"ods product_subcategory", {"ProductSubcategoryKey"}, "ods product_subcategory", JoinKind.LeftOuter),
  #"Expanded ods product_subcategory" = Table.ExpandTableColumn(#"Merged queries", "ods product_subcategory", {"SubcategoryName", "CategoryName"}, {"SubcategoryName", "CategoryName"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded ods product_subcategory", {"ProductSubcategoryId"})
in
  #"Removed columns";
shared #"ods product_subcategory" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "product_subcategory"]}[Data],
  #"Merged queries" = Table.NestedJoin(#"Navigation 2", {"ProductCategoryKey"}, #"ods product_category", {"ProductCategoryKey"}, "ods product_category", JoinKind.LeftOuter),
  #"Expanded ods product_category" = Table.ExpandTableColumn(#"Merged queries", "ods product_category", {"CategoryName"}, {"CategoryName"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded ods product_category", {"ProductCategoryKey"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"ProductSubcategoryKey", Int64.Type}})
in
  #"Changed column type";
shared #"ods product_category" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "product_category"]}[Data]
in
  #"Navigation 2";
shared #"stg product_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  TableNavigation = Navigation_2{[Schema = "stg", Item = "product"]}[Data]
in
  TableNavigation;
