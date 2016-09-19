###########################################
# get_cfsv2_ts_ncdc.r
# pulls cfsv2 forecasts from NCDC timeseries archive
# subsets to gbm and africa domains
# pulls out specified forecast variables
###########################################

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
