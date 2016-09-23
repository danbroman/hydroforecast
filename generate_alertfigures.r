#######DESCRIPTION###############################
# generate_alertfigures.r
# produces spatial maps of satellite-derived rainfall
# timeseries of rainfall, and timeseries of discharge
# to accompany a flood alert for a selected
# river basin
#################################################

## load libraries
library(data.table)
library(dplyr)	
library(lubridate)
library(readr)
library(tidyr)
library(stringr)
library(ncdf4)
library(ggplot2)
library(RColorBrewer)
library(rgdal)
library(maptools)
library(leaflet)
library(rgeos)
library(ggmap)
library(PBSmapping)
library(png)
library(grid)
library(gridExtra)

Matlab2Rdate = function(val) as.Date(val - 1, origin = '0000-01-01') 
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
      sep="", collapse=" ")
}

## read in data
setwd('')
dir_plots = ''
