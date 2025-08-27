[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ods customer_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Prefix", DestinationColumnName = "Prefix"], [SourceColumnName = "FirstName", DestinationColumnName = "FirstName"], [SourceColumnName = "LastName", DestinationColumnName = "LastName"], [SourceColumnName = "BirthDate", DestinationColumnName = "BirthDate"], [SourceColumnName = "MaritalStatus", DestinationColumnName = "MaritalStatus"], [SourceColumnName = "Gender", DestinationColumnName = "Gender"], [SourceColumnName = "EmailAddress", DestinationColumnName = "EmailAddress"], [SourceColumnName = "AnnualIncome", DestinationColumnName = "AnnualIncome"], [SourceColumnName = "EducationLevel", DestinationColumnName = "EducationLevel"], [SourceColumnName = "Occupation", DestinationColumnName = "Occupation"], [SourceColumnName = "HomeOwner", DestinationColumnName = "HomeOwner"], [SourceColumnName = "CustomerId", DestinationColumnName = "CustomerId"], [SourceColumnName = "TotalChildren", DestinationColumnName = "TotalChildren"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"ods customer" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "ods", Item = "customer"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"CustomerKey", Int64.Type}, {"TotalChildren", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"CustomerKey", "CustomerId"}})
in
  #"Renamed columns";
shared #"ods customer_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  TableNavigation = Navigation_2{[Schema = "stg", Item = "customer"]}[Data]
in
  TableNavigation;
