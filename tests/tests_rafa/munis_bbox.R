# gera bbox de estados e municipios

library(sf)
library(dplyr)

# Load the state polygons
df <- geobr::read_municipality(year = 2022)

# Calculate bounding boxes of states
bounding_boxes <- df |>
  st_as_sf() |>                           # Ensure df is an sf object
  rowwise() |>                            # Process each polygon individually
  mutate(
    xmin = st_bbox(geom)["xmin"],      # Extract xmin from the bounding box
    ymin = st_bbox(geom)["ymin"],      # Extract ymin from the bounding box
    xmax = st_bbox(geom)["xmax"],      # Extract xmax from the bounding box
    ymax = st_bbox(geom)["ymax"]       # Extract ymax from the bounding box
  ) |>
  ungroup() |>                            # Unrowwise after rowwise operations
  select(code_muni, xmin, ymin, xmax, ymax) |> # Select desired columns
  st_drop_geometry()

# View the resulting bounding box data.frame
head(bounding_boxes)

data.table::fwrite(bounding_boxes, './inst/extdata/munis_bbox.csv')


head(input_table)

candidate_states <-
  subset(x = bounding_boxes,
         (xmin < bbox_lon_min | xmax > bbox_lon_max) &
           (ymin < bbox_lat_min | ymax > bbox_lat_max)
  )


