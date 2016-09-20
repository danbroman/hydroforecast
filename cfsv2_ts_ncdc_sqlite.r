###########################################
# cfsv2_ts_ncdc_sqlite.r
# processes grib2 files from ncdc cfsv2 archive
# saves data to sqlite database
# saves data in rdata format
###########################################

library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)
library(RSQLite)
library(sqldf)

dir_scratch = '/d1/dbroman/projects/cfsv2/scratch/'
dir_gbm = '/d1/dbroman/projects/cfsv2/grib2/gbm/'
dir_gbm_proc = '/d1/dbroman/projects/cfsv2/rdata/gbm/'

setwd(dir_gbm_proc)
db = dbConnect(SQLite(), dbname = 'gbm_cfsv2.sqlite')

gbm_file_list = list.files(dir_gbm, pattern = '*.grb2')
nfiles_gbm = length(gbm_file_list)

for(i in 1:nfiles_gbm){
  gbm_file_sel = paste0(dir_gbm, gbm_file_list[i])
	system(paste0("wgrib2 ", gbm_file_sel, " -csv temp.csv"))
	tryCatch({
		fcst_dt_temp = fread('temp.csv') %>% setnames(c('datetime_init', 'datetime_fcst', 'var', 'level', 'lon', 'lat', 'value'))
		fcst_dt_temp = fcst_dt_temp %>% mutate(datetime_init = as.POSIXct(datetime_init), datetime_fcst = as.POSIXct(datetime_fcst)) %>% mutate(hour_fcst = hour(datetime_fcst), hour_init = hour(datetime_init)) %>% mutate(date_init = as.Date(datetime_init), date_fcst = as.Date(datetime_fcst))
		fcst_dt_temp_24z = fcst_dt_temp %>% filter(hour_fcst == 0) %>% mutate(date_fcst = date_fcst - days(1), hour_fcst = 24)
		fcst_dt_temp = bind_rows(fcst_dt_temp, fcst_dt_temp_24z) %>% mutate(date_init = as.character(date_init), date_fcst = as.character(date_fcst)) %>% mutate(datetime_init = as.character(datetime_init), datetime_fcst = as.character(datetime_fcst)) %>% select(datetime_init, date_init, hour_init, datetime_fcst, date_fcst, hour_fcst, var, level, lon, lat, value) %>% data.table()
		var_temp = unique(fcst_dt_temp$var)
  		dbWriteTable(conn = db, name = var_temp, value = fcst_dt_temp, row.names = F, append = T)
	})
}


dir_scratch = '/d1/dbroman/projects/cfsv2/scratch/'
dir_africa = '/d1/dbroman/projects/cfsv2/grib2/africa/'
dir_africa_proc = '/d1/dbroman/projects/cfsv2/rdata/africa/'

setwd(dir_africa_proc)
db = dbConnect(SQLite(), dbname = 'africa_cfsv2.sqlite')

africa_file_list = list.files(dir_africa, pattern = '*.grb2')
nfiles_africa = length(africa_file_list)

for(i in 1:nfiles_africa){
  africa_file_sel = paste0(dir_africa, africa_file_list[i])
	system(paste0("wgrib2 ", africa_file_sel, " -csv temp.csv"))
	tryCatch({
		fcst_dt_temp = fread('temp.csv') %>% setnames(c('datetime_init', 'datetime_fcst', 'var', 'level', 'lon', 'lat', 'value'))
		fcst_dt_temp = fcst_dt_temp %>% mutate(datetime_init = as.POSIXct(datetime_init), datetime_fcst = as.POSIXct(datetime_fcst)) %>% mutate(hour_fcst = hour(datetime_fcst), hour_init = hour(datetime_init)) %>% mutate(date_init = as.Date(datetime_init), date_fcst = as.Date(datetime_fcst))
		fcst_dt_temp_24z = fcst_dt_temp %>% filter(hour_fcst == 0) %>% mutate(date_fcst = date_fcst - days(1), hour_fcst = 24)
		fcst_dt_temp = bind_rows(fcst_dt_temp, fcst_dt_temp_24z) %>% mutate(date_init = as.character(date_init), date_fcst = as.character(date_fcst)) %>% mutate(datetime_init = as.character(datetime_init), datetime_fcst = as.character(datetime_fcst)) %>% select(datetime_init, date_init, hour_init, datetime_fcst, date_fcst, hour_fcst, var, level, lon, lat, value) %>% data.table()
		var_temp = unique(fcst_dt_temp$var)
  		dbWriteTable(conn = db, name = var_temp, value = fcst_dt_temp, row.names = F, append = T)
	})
}
