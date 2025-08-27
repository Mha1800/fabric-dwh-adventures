[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"stg sales" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "stg", Item = "sales"]}[Data],
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Navigation 2", "processed_dttm", each datetime()), {{"processed_dttm", type datetime}})
in
  #"Added custom";
shared #"dwh sales" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6db8acd3-f954-465c-b74e-6c155204510c"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "e68688dd-190e-4451-9c06-b46d7c5e06ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dwh", Item = "sales"]}[Data],
  #"Appended query" = Table.Combine({#"Navigation 2", #"stg sales"})
in
  #"Appended query";
