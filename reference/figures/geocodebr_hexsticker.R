library(hexSticker) # https://github.com/GuangchuangYu/hexSticker
library(ggplot2)
library(sf)
library(geobr)
library(ggimage)
library(sfheaders)


# add special text font
library(sysfonts)
font_add_google(name = "Roboto", family = "Roboto")
# font_add_google(name = "HelveticaR", family = "Linotype")

library(extrafont)
# font_import()
loadfonts(device = "win")

# Download shape files
br <- geobr::read_country(simplified = TRUE)

# Simplify geometry
br_s <- br |> sf::st_transform(crs=3857) |>
  sf::st_simplify( preserveTopology = T, dTolerance = 10000)

# plot(st_sample(st_cast(br_s$geom[1],"MULTILINESTRING"), 30))

# sample points
set.seed(42)
n_points <- 120
points_sf <- sf::st_sample(br_s, size = n_points, type='hexagonal')
points_sf <- sf::st_transform(points_sf, crs=4674)
points_df <- sfheaders::sfc_to_df(points_sf)
points_df$image <- "./man/figures/location_icon.png"
#points_df$image <- "person_icon_black.png"
points_df$fsize <- sample(c(.02,.03, .05), size = nrow(points_df), replace = T)

### build logo ---------------

### .png
ccol <- '#ff7c43' # '#5c997e' # '#79af97'
plot_y <-
  ggplot() +
  geom_sf(data=sf::st_transform(br_s, crs=4674), color=NA, fill=ccol) +
  geom_image(data=subset(points_df, fsize==.02), aes(x, y, image=image),  size=.02) +
  geom_image(data=subset(points_df, fsize==.03), aes(x, y, image=image),  size=.03) +
  geom_image(data=subset(points_df, fsize==.05), aes(x, y, image=image),  size=.05) +
  # geom_point(data=points_df, aes(x, y, image=image, size=fsize)) +
  theme_void()



# plot_y

ffont <- 'sans' # sans  Montserrat  Roboto
sticker(plot_y,

        # package name
        package= 'geocodebr', p_size=4, p_x = .51, p_y = .73, p_color = "white",
        p_family=ffont, p_fontface='bold',

        # ggplot image size and position
        s_x=1.12, s_y=.9, s_width=1.8, s_height=1.8,

        # hexagon
        h_fill=ccol, h_color=NA,

        # url
        url = "github.com/ipeaGIT/geocodebr", u_color= "white",
         u_size = 4, u_family=ffont,
        # u_x = 1.2, u_y = .19,

        # save output name and resolution
        filename="./man/figures/geocodebr_logo.png", dpi=400
        )

  beepr::beep()



# ### .svg
# plot_y_svg <-
#   ggplot() +
#   geom_sf(data = meso_s, fill=NA, size=.08, color="#272D67") +
#   geom_sf(data = uf_s, fill=NA, size=.35, color="#2E3946") +
#   theme_void() +
#   theme(panel.grid.major=element_line(colour="transparent")) +
#   #  theme(legend.position = "none") +
#   annotate("text", x = -67.7, y = -20, label= "geobr", color="black",
#            size = 6, family = "Roboto", fontface="bold", angle = 0) # (.png  size = 25)(.svg  size = 6)
#
#
#
# sticker(plot_y_svg, package="",
#         s_x=1.12, s_y=.9, s_width=1.8, s_height=1.8, # ggplot image size and position
#         h_fill="#FEB845", h_color="#FE9F45", # hexagon
#         filename="./man/figures/geobr_logo_y2.svg")  # output name and resolution
#



