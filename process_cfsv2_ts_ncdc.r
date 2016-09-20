###########################################
# process_cfsv2_ts_ncdc.r
# processes grib2 files from ncdc cfsv2 archive
# aggregates 6-hrly forecasts to 24hr and 5-day accumulations
# saves data in rdata format
###########################################

library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)

dir_scratch = '/d1/dbroman/projects/cfsv2/scratch/'
dir_gbm = '/d1/dbroman/projects/cfsv2/grib2/gbm/'
dir_africa = '/d1/dbroman/projects/cfsv2/grib2/africa/'

dir_gbm_proc = '/d1/dbroman/projects/cfsv2/rdata/gbm/'
dir_africa_proc = '/d1/dbroman/projectscfsv2/rdata/africa/'

rdata_file_gbm = 'prate_fcst_gbm_24hraccum.rda'
rdata_file_africa = 'prate_fcst_africa_24hraccum.rda'

setwd(dir_scratch)

gbm_file_list = list.files(dir_gbm, pattern = '*.grb2')
nfiles_gbm = length(gbm_file_list)
# 21600 sec / 6hr
weight_tbl = data.table(fcst_hour = c(0, 6, 12, 18, 24), weight = c(0.5, 1, 1, 1, 0.5))
for(i in 1:nfiles_gbm){
	gbm_file_sel = paste0(dir_gbm, gbm_file_list[i])
	system(paste0("wgrib2 ", gbm_file_sel, " -csv temp.csv"))
	fcst_dat = fread('temp.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value')) %>% select(-var, -level)
	fcst_dat = fcst_dat  %>% mutate(date_init = as.POSIXct(date_init), date_fcst = as.POSIXct(date_fcst), value = value * 21600) %>% mutate(fcst_hour = hour(date_fcst)) %>% mutate(date_init = as.Date(date_init), date_fcst = as.Date(date_fcst))

	fcst_dat_24z = fcst_dat %>% filter(fcst_hour == 0) %>% mutate(date_fcst = date_fcst - days(1), fcst_hour = 24)

	fcst_dat = bind_rows(fcst_dat, fcst_dat_24z)
	fcst_dat_24hraccum = fcst_dat %>% left_join(weight_tbl) %>% mutate(value = value * weight, date_fcst = date_fcst + days(1)) %>% group_by(date_init, date_fcst, lon, lat) %>% dplyr::summarise(value = sum(value)) 

	if(file.exists(paste0(dir_gbm_proc, rdata_file_gbm)) == T){
		data_proc_temp = readRDS(paste0(dir_gbm_proc, rdata_file_gbm))
		data_proc_temp = bind_rows(data_proc_temp, fcst_dat)
		saveRDS(data_proc_temp, paste0(dir_gbm_proc, rdata_file_gbm))
	}
	if(file.exists(paste0(dir_gbm_proc, rdata_file_gbm)) == F){
		saveRDS(fcst_dat, paste0(dir_gbm_proc, rdata_file_gbm))
	}
}

for(i in 1:nfiles_africa){
	africa_file_sel = paste0(dir_africa, africa_file_list[i])
	system(paste0("wgrib2 ", africa_file_sel, " -csv temp.csv"))
	fcst_dat = fread('temp.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value')) %>% select(-var, -level)
	fcst_dat = fcst_dat  %>% mutate(date_init = as.POSIXct(date_init), date_fcst = as.POSIXct(date_fcst), value = value * 21600) %>% mutate(fcst_hour = hour(date_fcst)) %>% mutate(date_init = as.Date(date_init), date_fcst = as.Date(date_fcst))

	fcst_dat_24z = fcst_dat %>% filter(fcst_hour == 0) %>% mutate(date_fcst = date_fcst - days(1), fcst_hour = 24)

	fcst_dat = bind_rows(fcst_dat, fcst_dat_24z)
	fcst_dat_24hraccum = fcst_dat %>% left_join(weight_tbl) %>% mutate(value = value * weight, date_fcst = date_fcst + days(1)) %>% group_by(date_init, date_fcst, lon, lat) %>% dplyr::summarise(value = sum(value)) 

	if(file.exists(paste0(dir_africa_proc, rdata_file_africa)) == T){
		data_proc_temp = readRDS(paste0(dir_africa_proc, rdata_file_africa))
		data_proc_temp = bind_rows(data_proc_temp, fcst_dat)
		saveRDS(data_proc_temp, paste0(dir_africa_proc, rdata_file_africa))
	}
	if(file.exists(paste0(dir_africa_proc, rdata_file_africa)) == F){
		saveRDS(fcst_dat, paste0(dir_africa_proc, rdata_file_africa))
	}
}
