library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)
library(doParallel)
library(foreach)

dir_scratch = ''
dir_gbm = ''
dir_africa = ''


lims_lon_gbm = c(73, 98)
lims_lat_gbm = c(22, 32)
lims_lon_africa = c(-20, 55)
lims_lat_africa = c(-40, 40)
var_sel = 'prate'
fcst_lead_sel = 1440


time_sel_start = as.POSIXct('2011-04-01', tz = 'utc')
time_sel_end = as.POSIXct('2016-09-15 18:00', tz = 'utc')


# time_list = seq(from = time_sel_start, to = time_sel_end, by = 'day')
time_list = seq(from = time_sel_start, to = time_sel_end, by = '6 hour')

ntimes = length(time_list)
setwd(dir_scratch)
tempvar_list = rep(letters, ceiling(ntimes / 26))

get_cfs_ts_grb = function(var, time_sel, fcst_lead = 1440, tempvar){
	#saves in grb format
	require(data.table)
	require(dplyr)
	require(lubridate)
	require(stringr)
	require(tidyr)
	timenow = time_sel
	# as.POSIXct(format(time_sel, tz = 'utc'))
	yearnow = year(timenow)
	monthnow = str_pad(month(timenow), 2, pad = "0")
	daynow = str_pad(day(timenow), 2, pad = "0")
	hournow = str_pad(hour(timenow), 2, pad = "0")

	initdatefilestr = paste0(yearnow, monthnow, daynow)
	initdatestr = paste0(yearnow, monthnow, daynow, hournow)
	fcst_lead_list = seq(from = 6, to = fcst_lead, by = 6)
	fcst_match_list = paste(paste0(':', fcst_lead_list, ' hour fcst:'), collapse = '|')
	url = paste0('http://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/', yearnow, '/', paste0(yearnow, monthnow), '/', initdatefilestr, '/', initdatestr, '/', var, '.', '01', '.', initdatestr, '.daily.grb2') 
	destfile_gbm = paste0(dir_gbm, var, '.', initdatestr, '.', '01', '.grb2') 
	destfile_africa = paste0(dir_africa, var, '.', initdatestr, '.', '01', '.grb2') 
	#checks filesize if existing and removes if missing forecast times
	if(file.exists(destfile_gbm) == T){
		file_meta = data.table(raw = system(paste('wgrib2', destfile_gbm, '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		if(file_max_hour < fcst_lead){
			system(paste("rm", destfile_gbm))
		}
	}
	if(file.exists(destfile_africa) == T){
		file_meta = data.table(raw = system(paste('wgrib2', destfile_africa, '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		if(file_max_hour < fcst_lead){
			system(paste("rm", destfile_africa))
		}
	}

	#downloads and subsets (if needed)
	if(file.exists(destfile_gbm) == F){
		download.file(url, paste0('temp_', tempvar, '.grb2'), mode = 'wb')
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match '", fcst_match_list, "' -g2clib 0 -small_grib ", paste(lims_lon_gbm, collapse = ':'), " ", paste(lims_lat_gbm, collapse = ':'), " ", destfile_gbm), ignore.stdout = T, ignore.stderr = T)
		if(file.exists(destfile_africa) == F){
			system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match '", fcst_match_list, "' -g2clib 0 -small_grib ", paste(lims_lon_africa, collapse = ':'), " ", paste(lims_lat_africa, collapse = ':'), " ", destfile_africa), ignore.stdout = T, ignore.stderr = T)
		}
	}
	if(file.exists(destfile_africa) == F){
		download.file(url, paste0('temp_', tempvar, '.grb2'), mode = 'wb')
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match '", fcst_match_list, "' -g2clib 0 -small_grib ", paste(lims_lon_africa, collapse = ':'), " ", paste(lims_lat_africa, collapse = ':'), " ", destfile_africa), ignore.stdout = T, ignore.stderr = T)
	}
}


cl = makeCluster(7)
registerDoParallel(cl)
foreach (i = 1:ntimes) %dopar% {
	time_sel = time_list[i]
	tempvar_sel = tempvar_list[i]
	try(get_cfs_ts_grb(var_sel, time_sel, fcst_lead_sel, tempvar_sel))
}
stopCluster(cl)

###############################
# file checking
require(tools)
file_list_gbm = sort(list.files(dir_gbm, pattern = '*grb.'))
nfiles_gbm = length(file_list_gbm)
# nfiles_gbm = 2565
file_gbm_dt = data.table()
for(i in 1:nfiles_gbm){
	file_temp = file_list_gbm[i]
	file_meta = data.table(raw = system(paste('wgrib2', paste0(dir_gbm, file_temp), '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		file_nfcst = nrow(file_meta)
	file_info = file_path_sans_ext(file_temp)
	file_gbm_dt_temp = data.table(file = file_temp, fcst_hour_max = file_max_hour, nfcst_step = file_nfcst, info = file_info) %>% separate(info, sep = '\\.', into = c('var', 'date', 'ensemble')) %>% mutate(date = as.POSIXct(date, format = '%Y%m%d%H', tz = 'utc'))
	file_gbm_dt = bind_rows(file_gbm_dt, file_gbm_dt_temp)
}

test = file_gbm_dt %>% mutate(year = year(date)) %>% select(file, date) %>% data.table()
# %>% filter(year == 2012) 
test_fl = data.table(date = time_list)

test_fl = left_join(test_fl, test)
test_flmissdate = test_fl %>% filter(is.na(file)) %>% mutate(year = year(date)) %>% filter(year != 2015)
write.csv(test_flmissdate, 'gbm_missdates.csv', row.names = F)


file_list_africa = sort(list.files(dir_africa, pattern = '*grb.'))
nfiles_africa = length(file_list_africa)
nfiles_africa = 1200
file_africa_dt = data.table()
for(i in 1: nfiles_africa){
	file_temp = file_list_africa[i]
	file_meta = data.table(raw = system(paste('wgrib2', paste0(dir_africa, file_temp), '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		file_nfcst = nrow(file_meta)
	file_info = file_path_sans_ext(file_temp)
	file_africa_dt_temp = data.table(file = file_temp, fcst_hour_max = file_max_hour, nfcst_step = file_nfcst, info = file_info) %>% separate(info, sep = '\\.', into = c('var', 'date', 'ensemble')) %>% mutate(date = as.POSIXct(date, format = '%Y%m%d%H', tz = 'utc'))
	file_africa_dt = bind_rows(file_africa_dt, file_africa_dt_temp)
}

test = file_africa_dt %>% mutate(year = year(date)) %>% filter(year == 2011) %>% select(file, date) %>% data.table()

test_fl = data.table(date = time_list)

test_fl = left_join(test_fl, test)
test_flmissdate = test_fl %>% filter(is.na(file))



nohup Rscript get_cfsv2_ts_11.r > getcfsv2_ts_11.log 2>&1 &
nohup Rscript get_cfsv2_ts_12.r > getcfsv2_ts_12.log 2>&1 &
nohup Rscript get_cfsv2_ts_13.r > getcfsv2_ts_13.log 2>&1 &
nohup Rscript get_cfsv2_ts_14.r > getcfsv2_ts_14.log 2>&1 &
nohup Rscript get_cfsv2_ts_16.r > getcfsv2_ts_16.log 2>&1 &

nohup Rscript get_cfsv2_ts_parallel.r > getcfsv2_ts_parallel.log 2>&1 &

nohup Rscript get_cfsv2_ts.r > getcfsv2_ts.log 2>&1 &
########################################
# process forecast data
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




test = fcst_dat_24hraccum %>% filter(date_fcst == '2012-02-10')
ggplot() + geom_raster(data =test, aes(x = lon, y = lat, fill = value))



Matlab2Rdate <- function(val) as.Date(val - 1, origin = '0000-01-01') 
Matlab2Rdate(733774)
