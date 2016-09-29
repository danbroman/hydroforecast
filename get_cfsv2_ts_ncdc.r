###########################################
# get_cfsv2_ts_ncdc.r
# pulls cfsv2 forecasts from NCDC timeseries archive
# subsets to ext and africa domains
# pulls out specified forecast variables
###########################################

start_time()
## load libraries 
library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)
library(doParallel)
library(foreach)

## user inputs
dir_scratch = ''
dir_ext = ''

lims_lon_ext = c(73, 98)
lims_lat_ext = c(22, 32)

fcst_lead_sel = 1440
time_sel_start = as.POSIXct('2011-04-01', tz = 'utc')
time_sel_end = as.POSIXct('2016-09-15 18:00', tz = 'utc')

var_sel = 'prate'

ncores_sel = 6

## setup
setwd(dir_scratch)

time_list = seq(from = time_sel_start, to = time_sel_end, by = '6 hour')
ntimes = length(time_list)

tempvar_list = rep(letters, ceiling(ntimes / 26))

## download function
get_cfs_ts_grb = function(var, time_init_sel, fcst_lead = 1440, tempvar){
	require(data.table)
	require(dplyr)
	require(lubridate)
	require(stringr)
	require(tidyr)
	
	urlhead = 'http://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/'
	
	yearinit = year(time_init_sel)
	monthinit = str_pad(month(time_init_sel), 2, pad = "0")
	dayinit = str_pad(day(time_init_sel), 2, pad = "0")
	hourinit = str_pad(hour(time_init_sel), 2, pad = "0")

	initdatefilestr = paste0(yearinit, monthinit, dayinit)
	initdatestr = paste0(yearinit, monthinit, dayinit, hourinit)
	inityrmon = paste0(yearinit, monthinit)
	
	fcst_lead_list = seq(from = 6, to = fcst_lead, by = 6)
	fcst_match_list = paste(paste0(':', fcst_lead_list, ' hour fcst:'), collapse = '|')
	
	url = paste0(urlhead, yearinit, '/', inityrmon, '/', initdatefilestr, '/', initdatestr, '/', var, '.', '01', '.', initdatestr, '.daily.grb2') 
	
	destfile_ext = paste0(dir_ext, var, '.', initdatestr, '.', '01', '.grb2') 
	
	#checks forecast lead against selected and removes file if too small
	if(file.exists(destfile_ext) == T){
		file_meta = data.table(raw = system(paste('wgrib2', destfile_ext, '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		if(file_max_hour < fcst_lead){
			system(paste("rm", destfile_ext))
		}
	}
	
	#downloads and subsets (if needed)
	if(file.exists(destfile_ext) == F){
		download.file(url, paste0('temp_', tempvar, '.grb2'), mode = 'wb')
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match '", fcst_match_list, "' -g2clib 0 -small_grib ", paste(lims_lon_ext, collapse = ':'), " ", paste(lims_lat_ext, collapse = ':'), " ", destfile_ext), ignore.stdout = T, ignore.stderr = T)
	}
}

## call function 
cl = makeCluster(ncores_sel)
registerDoParallel(cl)
foreach (i = 1:ntimes) %dopar% {
	time_init_sel = time_list[i]
	tempvar_sel = tempvar_list[i]
	try(get_cfs_ts_grb(var_sel, time_init_sel, fcst_lead_sel, tempvar_sel))
}
stopCluster(cl)
Sys.time() - start_time

#removes small files...if download didn't complete etc.
system(paste("find", dir_ext, "-size -1k -delete"))
