###########################################
# process_cfsv2_flxf_ncdc.r
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

str2date = function(str_date){
	as.POSIXct(paste0(substr(str_date, 1, 4), '-', substr(str_date, 5, 6), '-', substr(str_date, 7, 8), ' ', substr(str_date, 9, 10), ':00'), tz = 'utc')
}

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
c_file = fread('')

# extension for outfile
fileout_dom = '_ethiopia_24hraccum'

var_sel = 'prate'
fcst_lead_sel = 1440
## set up
setwd(dir_scratch)

dom_file_list = list.files(dir_dom, pattern = '*.grb2')

dom_file_dt = data.table(file = dom_file_list) %>% separate(file, into = c('date_init', 'ens', 'date_fcst', 'ext'), sep = '\\.', remove = F)
date_init_list = unique(dom_file_dt$date_init)
nfiles_init = length(date_init_list)
# 21600 sec / 6hr
weight_tbl = data.table(fcst_hour = c(0, 6, 12, 18, 24), weight = c(0.5, 1, 1, 1, 0.5))

## process files
for(i in 1:nfiles_init){
	date_init_temp = date_init_list[i]
	file_list_temp = filter(dom_file_dt, date_init == date_init_temp)$file
	nfiles = length(file_list_temp)
	dom_file_sel = paste0(dir_dom, dom_file_list[i])
	file_out = paste0(dir_dom_proc, var_sel, '.',  date_init_temp, '.01', fileout_dom, '.csv')
	if(file.exists(file_out) == F){
		fcst_dat = NULL
		for(j in 1:nfiles){
			dom_file_sel = paste0(dir_dom, file_list_temp[j])
			system(paste0("wgrib2 ", dom_file_sel, " -match ':(", toupper(var_sel), "):' -csv temp2.csv"))
			tryCatch({
				fcst_dat_temp = fread('temp2.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value')) %>% select(-var, -level) %>% filter(lon >= lon_dom[1], lon <= lon_dom[2], lat >= lat_dom[1], lat <= lat_dom[2])
				fcst_dat = bind_rows(fcst_dat, fcst_dat_temp)
			})
		}
		fcst_dat = fcst_dat %>% filter(lon >= lon_dom[1], lon <= lon_dom[2], lat >= lat_dom[1], lat <= lat_dom[2]) %>% mutate(date_init = as.POSIXct(date_init, tz = 'utc'), date_fcst = as.POSIXct(date_fcst, tz = 'utc'), value = value * 21600)
		nfcst_files = length(unique(fcst_dat$date_fcst))

		# complete table to check for missing values
		if(nfcst_files < fcst_lead_sel / 6){
			fcst_dat_tp = data.table(date_init = str2date(date_init_temp), date_fcst = date_fcst_temp, lon = rep(lon_temp, each = nfcst), lat = rep(lat_temp, each = nfcst * nlon))
			fcst_dat = fcst_dat_tp %>% left_join(data.table(fcst_dat))
				lon_temp = unique(fcst_dat$lon)
			lat_temp = unique(fcst_dat$lat) 
			date_fcst_temp = seq(from = str2date(date_init_temp) + hours(6), by = '6 hours', length.out = 240)
			nlon = length(lon_temp)
			nlat = length(lat_temp)
			nfcst = length(date_fcst_temp)
		}
		
		fcst_dat = fcst_dat %>% mutate(fcst_hour = hour(date_fcst)) %>% mutate(date_init = as.Date(date_init), date_fcst = as.Date(date_fcst))

		fcst_dat_24z = fcst_dat %>% filter(fcst_hour == 0) %>% mutate(date_fcst = date_fcst - days(1), fcst_hour = 24)

		fcst_dat = bind_rows(fcst_dat, fcst_dat_24z)
		fcst_dat_24hraccum = fcst_dat %>% left_join(weight_tbl) %>% mutate(value = value * weight, date_fcst = date_fcst + days(1)) %>% group_by(date_init, date_fcst, lon, lat) %>% dplyr::summarise(value = sum(value)) 
			date_init_temp = unique(fcst_dat_24hraccum $date_init)
			date_fcst_list = sort(unique(fcst_dat_24hraccum$date_fcst))
		date_fcst_list = unique(fcst_dat_24hraccum$date_fcst)	
		nfcst = length(date_fcst_list)
		fcst_dat_24hraccum_interp = NULL
		for(j in 1:nfcst){
			date_fcst_temp = date_fcst_list[j]
				fcst_dat_24hraccum_fl = filter(fcst_dat_24hraccum, date_fcst == date_fcst_temp)
			x_temp = fcst_dat_24hraccum_fl$lon
			y_temp = fcst_dat_24hraccum_fl$lat
			z_temp = fcst_dat_24hraccum_fl$value
			interp_raw = interp(x_temp, y_temp, z_temp, xo = xp1, yo = yp1)
			fcst_dat_24hraccum_interp_temp = data.table(date_init = date_init_temp, date_fcst = date_fcst_temp, lon = interp_raw$x, lat = rep(interp_raw$y, each = length(interp_raw$x)), value = as.numeric(interp_raw$z)) %>% mutate(value = ifelse(value < 0, 0, value))%>% filter(!is.na(value))
			fcst_dat_24hraccum_interp = bind_rows(fcst_dat_24hraccum_interp, fcst_dat_24hraccum_interp_temp)
		}
		fcst_dat_24hraccum_basin = left_join(fcst_dat_24hraccum_interp, c_file) %>% filter(!is.na(name)) %>% group_by(date_init, date_fcst, ethbasin, ethbasin_I, name) %>% summarise(value = mean(value))
		write.csv(fcst_dat_24hraccum_basin, file_out)
	}
}
