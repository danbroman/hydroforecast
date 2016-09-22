###########################################
# process_cfsv2_ts_ncdc.r
# processes grib2 files from ncdc cfsv2 archive
# aggregates 6-hrly forecasts to 24hr accumulations
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

## user inputs
dir_scratch = ''
dir_dom = ''
dir_dom_proc = '/d1/dbroman/projects/cfsv2/rdata/africa/'

# define domain
lat_dom = c(2, 16)
lon_dom = c(31, 49)
fileout_dom = '_ethiopia_24hraccum.csv'

## setup
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
		system(paste0("wgrib2 ", dom_file_sel, " -csv temp.csv"))
		tryCatch({
			fcst_dat = fread('temp.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value')) %>% select(-var, -level)
			fcst_dat = fcst_dat %>% filter(lon >= lon_dom[1], lon <= lon_dom[2], lat >= lat_dom[1], lat <= lat_dom[2]) %>% mutate(date_init = as.POSIXct(date_init), date_fcst = as.POSIXct(date_fcst), value = value * 21600) %>% mutate(fcst_hour = hour(date_fcst)) %>% mutate(date_init = as.Date(date_init), date_fcst = as.Date(date_fcst))

			fcst_dat_24z = fcst_dat %>% filter(fcst_hour == 0) %>% mutate(date_fcst = date_fcst - days(1), fcst_hour = 24)

			fcst_dat = bind_rows(fcst_dat, fcst_dat_24z)
			fcst_dat_24hraccum = fcst_dat %>% left_join(weight_tbl) %>% mutate(value = value * weight, date_fcst = date_fcst + days(1)) %>% group_by(date_init, date_fcst, lon, lat) %>% dplyr::summarise(value = sum(value)) 
			write.csv(fcst_dat_24hraccum, file_out)
		})
	}
}
