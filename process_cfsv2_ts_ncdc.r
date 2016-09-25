###########################################
# process_cfsv2_ts_ncdc.r
# processes grib2 files from ncdc cfsv2 archive
# aggregates 6-hrly forecasts to 24hr and 5-day accumulations
# regrids and maps to river basins using correspondence file
# saves data in csv format
###########################################

## load libraries
library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)
library(tools)
library(akima)

## user inputs
dir_scratch = ''
dir_dom = ''
dir_dom_proc = ''

# define sub-domain of raw forecast
lat_dom = c(1, 16)
lon_dom = c(32, 49)

# new 0.1º grid
xp1 = seq(from = 33.05, by = 0.1, length.out = 150)
yp1 = seq(from = 2.05, by = 0.1, length.out = 130)

# correspondence file 
c_file = fread('correspondence_ethiopiabasins.csv')

# extension for outfile
fileout_dom = '_ethiopia_24hraccum.csv'

## set up
setwd(dir_scratch)

dom_file_list = list.files(dir_dom, pattern = '*.grb2')
nfiles_dom = length(dom_file_list)
# 21600 sec / 6hr
weight_tbl = data.table(fcst_hour = c(0, 6, 12, 18, 24), weight = c(0.5, 1, 1, 1, 0.5))

## process files
for(i in 1:nfiles_dom){
	dom_file_sel = paste0(dir_dom, dom_file_list[i])
	file_out = paste0(dir_dom_proc, file_path_sans_ext(dom_file_list[i]), fileout_dom)
	if(file.exists(file_out) == F){
		system(paste0("wgrib2 ", dom_file_sel, " -csv temp3.csv"))
		tryCatch({
			fcst_dat = fread('temp3.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value')) %>% select(-var, -level)
			fcst_dat = fcst_dat %>% filter(lon >= lon_dom[1], lon <= lon_dom[2], lat >= lat_dom[1], lat <= lat_dom[2]) %>% mutate(date_init = as.POSIXct(date_init), date_fcst = as.POSIXct(date_fcst), value = value * 21600) %>% mutate(fcst_hour = hour(date_fcst), init_hour = hour(date_init)) %>% mutate(date_init = as.Date(date_init), date_fcst = as.Date(date_fcst)) 

			fcst_dat_24z = fcst_dat %>% filter(fcst_hour == 0) %>% mutate(date_fcst = date_fcst - days(1), fcst_hour = 24)

			fcst_dat = bind_rows(fcst_dat, fcst_dat_24z) %>% group_by(lon, lat, date_fcst) %>% mutate(nfcst = n()) %>% filter(nfcst == 5)

			fcst_dat_24hraccum = fcst_dat %>% left_join(weight_tbl) %>% mutate(value = value * weight, date_fcst = date_fcst + days(1)) %>% group_by(date_init, init_hour, date_fcst, lon, lat) %>% dplyr::summarise(value = sum(value)) 
			date_init_temp = unique(fcst_dat_24hraccum$date_init)
			init_hour_temp = unique(fcst_dat_24hraccum$init_hour)
			date_fcst_list = sort(unique(fcst_dat_24hraccum$date_fcst))
			nfcst = length(date_fcst_list)
			fcst_dat_24hraccum_interp = NULL
			for(j in 1:nfcst){
				date_fcst_temp = date_fcst_list[j]
				fcst_dat_24hraccum_fl = filter(fcst_dat_24hraccum, date_fcst == date_fcst_temp)
				x_temp = fcst_dat_24hraccum_fl$lon
				y_temp = fcst_dat_24hraccum_fl$lat
				z_temp = fcst_dat_24hraccum_fl$value
				interp_raw = interp(x_temp, y_temp, z_temp, xo = xp1, yo = yp1)
				fcst_dat_24hraccum_interp_temp = data.table(date_init = date_init_temp, hour_init = init_hour_temp, date_fcst = date_fcst_temp, lon = interp_raw$x, lat = rep(interp_raw$y, each = length(interp_raw$x)), value = as.numeric(interp_raw$z)) %>% mutate(value = ifelse(value < 0, 0, value))%>% filter(!is.na(value))
				fcst_dat_24hraccum_interp = bind_rows(fcst_dat_24hraccum_interp, fcst_dat_24hraccum_interp_temp)
			}
			fcst_dat_24hraccum_basin = left_join(fcst_dat_24hraccum_interp, c_file) %>% filter(!is.na(name)) %>% group_by(date_init, hour_init, date_fcst, ethbasin, ethbasin_I, name) %>% summarise(value = mean(value))
			write.csv(fcst_dat_24hraccum_basin, file_out)
		})
	}
}
