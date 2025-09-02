[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "stg sales_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "OrderDate", DestinationColumnName = "OrderDate"], [SourceColumnName = "product_rk", DestinationColumnName = "product_rk"], [SourceColumnName = "customer_rk", DestinationColumnName = "customer_rk"], [SourceColumnName = "territory_rk", DestinationColumnName = "territory_rk"], [SourceColumnName = "ProductId", DestinationColumnName = "ProductId"], [SourceColumnName = "CustomerId", DestinationColumnName = "CustomerId"], [SourceColumnName = "TerritoryId", DestinationColumnName = "TerritoryId"], [SourceColumnName = "StockDate", DestinationColumnName = "StockDate"], [SourceColumnName = "OrderNumber", DestinationColumnName = "OrderNumber"], [SourceColumnName = "OrderLineItem", DestinationColumnName = "OrderLineItem"], [SourceColumnName = "OrderQuantity", DestinationColumnName = "OrderQuantity"], [SourceColumnName = "processed_dttm", DestinationColumnName = "processed_dttm"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"stg sales" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "stg", Item = "sales"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "processed_dttm", each DateTime.LocalNow()), {{"processed_dttm", type datetime}})
in
  #"Added custom";
shared #"stg sales_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dwh", Item = "sales"]}[Data]
in
  TableNavigation;
