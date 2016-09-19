###########################################
# get_cfsv2_ncdc.r
# pulls cfsv2 forecasts from NCDC archive
# subsets to gbm and africa domains
# pulls out precip. surface temp, winds, and latent
# heat flux
###########################################

start_time = Sys.time()

## load libraries
library(stringr)
library(dplyr)
library(data.table)
library(lubridate)

date2str = function(dte){
	#dte - POSIXct date 
	year_str = year(dte)
	month_str = str_pad(month(dte), 2, pad = '0')
	day_str = str_pad(day(dte), 2, pad = '0')
	hour_str = str_pad(hour(dte), 2, pad = '0')

	paste0(year_str, month_str, day_str, hour_str)
}

## user inputs
savdir_scratch = ''
savdir_gbm = ''
savdir_africa = ''

dir_scratch = ''
dir_gbm = ''
dir_africa = ''

lims_lon_gbm = c(73, 98)
lims_lat_gbm = c(22, 32)
lims_lon_africa = c(-20, 55)
lims_lat_africa = c(-40, 40)

fcst_lead_sel = 1440
time_sel_start = as.POSIXct('2011-04-01', tz = 'utc')
time_sel_end = as.POSIXct('2011-12-31 18:00', tz = 'utc')

ncores_sel = 6

## setup
setwd(dir_scratch)

time_init_list = seq(from = time_sel_start, to = time_sel_end, by = '6 hour')
fcst_lead_list = seq(from = 6, to = fcst_lead_sel, by = 6)
nfcstlead = length(fcst_lead_list)
time_dt = data.table(time_init = rep(time_init_list, each = nfcstlead), fcst_lead = fcst_lead_list) %>% mutate(time_fcst = time_init + hours(fcst_lead))
ntimes = nrow(time_dt)

tempvar_list = rep(letters, ceiling(ntimes / 26))

## download function
get_cfs_grb = function(time_init_sel, time_fcst_sel, tempvar){
	require(data.table)
	require(dplyr)
	require(lubridate)
	require(stringr)
	require(tidyr)
	
	urlhead = 'http://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf/'
	
	yearinit = year(time_init_sel)
	monthinit = str_pad(month(time_init_sel), 2, pad = "0")
	dayinit = str_pad(day(time_init_sel), 2, pad = "0")
	hourinit = str_pad(hour(time_init_sel), 2, pad = "0")

	initdatefilestr = paste0(yearinit, monthinit, dayinit)
	initdatestr = paste0(yearinit, monthinit, daynow, hourinit)
	
	yeafcstr = year(time_fcst_sel)
	monthfcst = str_pad(month(time_fcst_sel), 2, pad = "0")
	dayfcst = str_pad(day(time_fcst_sel), 2, pad = "0")
	hourfcst = str_pad(hour(time_fcst_sel), 2, pad = "0")

	fcstdatestr = date2str(check_tbl[i, ]$fcst_date)

	url = paste0(urlhead, inityr, '/', inityrmon, '/', initdatefilestr, '/', initdatestr, '/flxf', fcstdatestr, '.01.', initdatestr, '.grb2')
	
	destfile_gbm = paste0(dir_gbm, fcstdatestr, '_', '01', '_', initdatestr ,'.grb2') 
	destfile_africa = paste0(dir_africa, fcstdatestr, '_', '01', '_', initdatestr ,'.grb2') 
	
	#downloads and subsets (if needed)
	if(file.exists(destfile_gbm) == F){
		download.file(url, paste0('temp_', tempvar, '.grb2'), mode = 'wb')
		
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match ":(TMP:2 m above ground|PRATE|CPRAT|LHTFL|UGRD:10 m above ground|VGRD:10 m above ground):" -small_grib ", paste(lims_lon_gbm, collapse = ':'), " ", paste(lims_lat_gbm, collapse = ':'), " ", destfile_gbm), ignore.stdout = T, ignore.stderr = T)
		
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match '", fcst_match_list, "' -g2clib 0 -small_grib ", paste(lims_lon_gbm, collapse = ':'), " ", paste(lims_lat_gbm, collapse = ':'), " ", destfile_gbm), ignore.stdout = T, ignore.stderr = T)
		if(file.exists(destfile_africa) == F){
			system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match ":(TMP:2 m above ground|PRATE|CPRAT|LHTFL|UGRD:10 m above ground|VGRD:10 m above ground):" -small_grib ", paste(lims_lon_africa, collapse = ':'), " ", paste(lims_lat_africa, collapse = ':'), " ", destfile_africa), ignore.stdout = T, ignore.stderr = T)
		}
	}
	if(file.exists(destfile_africa) == F){
		download.file(url, paste0('temp_', tempvar, '.grb2'), mode = 'wb')
		system(paste0('wgrib2 ', 'temp_', tempvar, '.grb2'," -match ":(TMP:2 m above ground|PRATE|CPRAT|LHTFL|UGRD:10 m above ground|VGRD:10 m above ground):" -small_grib ", paste(lims_lon_africa, collapse = ':'), " ", paste(lims_lat_africa, collapse = ':'), " ", destfile_africa), ignore.stdout = T, ignore.stderr = T)
	}
}

## call function
cl = makeCluster(ncores_sel)
registerDoParallel(cl)
foreach (i = 1:ntimes) %dopar% {
	time_init_sel = time_dt$time_init[i]
	time_fcst_sel = time_dt$time_fcst[i]
	tempvar_sel = tempvar_list[i]
	try(get_cfs_grb(time_init_sel, fcst_lead_sel, tempvar_sel))
}
stopCluster(cl)
Sys.time() - start_time
