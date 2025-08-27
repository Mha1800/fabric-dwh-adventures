[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ods sales_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "OrderDate", DestinationColumnName = "OrderDate"], [SourceColumnName = "product_rk", DestinationColumnName = "product_rk"], [SourceColumnName = "customer_rk", DestinationColumnName = "customer_rk"], [SourceColumnName = "territory_rk", DestinationColumnName = "territory_rk"], [SourceColumnName = "ProductId", DestinationColumnName = "ProductId"], [SourceColumnName = "CustomerId", DestinationColumnName = "CustomerId"], [SourceColumnName = "TerritoryId", DestinationColumnName = "TerritoryId"], [SourceColumnName = "StockDate", DestinationColumnName = "StockDate"], [SourceColumnName = "OrderNumber", DestinationColumnName = "OrderNumber"], [SourceColumnName = "OrderLineItem", DestinationColumnName = "OrderLineItem"], [SourceColumnName = "OrderQuantity", DestinationColumnName = "OrderQuantity"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"ods sales" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "sales"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"ProductKey", Int64.Type}, {"CustomerKey", Int64.Type}, {"TerritoryKey", Int64.Type}, {"OrderLineItem", type number}, {"OrderQuantity", type number}}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Changed column type", {{"OrderDate", type date}, {"StockDate", type date}}, "en-US"),
  #"Merged queries" = Table.NestedJoin(#"Changed column type with locale", {"CustomerKey"}, #"dwh customer_dim", {"CustomerId"}, "dwh customer_dim", JoinKind.LeftOuter),
  #"Expanded dwh customer_dim" = Table.ExpandTableColumn(#"Merged queries", "dwh customer_dim", {"customer_rk"}, {"customer_rk"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded dwh customer_dim", {"ProductKey"}, #"dwh product_dim", {"ProductId"}, "dwh product_dim", JoinKind.LeftOuter),
  #"Expanded dwh product_dim" = Table.ExpandTableColumn(#"Merged queries 1", "dwh product_dim", {"product_rk"}, {"product_rk"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded dwh product_dim", {"TerritoryKey"}, #"dwh territory_dim", {"TerritoryId"}, "dwh territory_dim", JoinKind.LeftOuter),
  #"Expanded dwh territory_dim" = Table.ExpandTableColumn(#"Merged queries 2", "dwh territory_dim", {"territory_rk"}, {"territory_rk"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded dwh territory_dim", {{"ProductKey", "ProductId"}, {"CustomerKey", "CustomerId"}, {"TerritoryKey", "TerritoryId"}})
in
  #"Renamed columns";
shared #"dwh customer_dim" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dwh", Item = "customer_dim"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"CustomerId"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"CustomerId"}),
  #"Choose columns" = Table.SelectColumns(#"Removed duplicates", {"customer_rk", "CustomerId"})
in
  #"Choose columns";
shared #"dwh product_dim" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dwh", Item = "product_dim"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"ProductId"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"ProductId"}),
  #"Choose columns" = Table.SelectColumns(#"Removed duplicates", {"ProductId", "product_rk"})
in
  #"Choose columns";
shared #"dwh territory_dim" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dwh", Item = "territory_dim"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"TerritoryId"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"TerritoryId"}),
  #"Choose columns" = Table.SelectColumns(#"Removed duplicates", {"TerritoryId", "territory_rk"})
in
  #"Choose columns";
shared #"ods sales_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  TableNavigation = Navigation_2{[Schema = "stg", Item = "sales"]}[Data]
in
  TableNavigation;
