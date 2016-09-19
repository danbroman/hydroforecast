###########################################
# get_cfsv2.r
# pulls cfsv2 forecasts from NCDC archive
# subsets to gbm and africa domains
# pulls out precip. surface temp, winds, and latent
# heat flux
# pulling all runs, 0-18z init times, out to 60 days (1440 hours)
###########################################
start_time = Sys.time()
library(stringr)
library(dplyr)
library(data.table)
library(lubridate)
# at inithour = 00z - 01 goes out 9mon 02-04 go out 1 season, inithour = 06-18z - 01 goes out 9mon, 02-04 out 45 days

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

start_date = '2015-01-01'
end_date = '2015-12-31'
runstr = c('01') # which ensemble members are desired - in NCDC archive only '01' member available
inithours = c(0, 6, 12, 18)
fcst_lead = 1440 # archive goes out a maximum of 9 months

## setup run
timeinit = as.POSIXct(format(end_date, tz = 'utc'))
timeinitcheck = as.POSIXct(format(start_date, tz = 'utc'))
fcsthours = seq(from = 0, to = fcst_lead, by = 6) 

## check files for last 2 days and fill in if needed
timeinitcheck_ls = seq(from = round(timeinitcheck, 'days'), to = timeinit, by = '6 hour')
check_tbl = data.table()
for(i in 1:length(timeinitcheck_ls)){
	check_tbl_temp = data.table(init_date = timeinitcheck_ls[i], fcst_date = timeinitcheck_ls[i] + hours(fcsthours_45day), run = rep(runstr, each = length(fcsthours)))
	check_tbl = bind_rows(check_tbl, check_tbl_temp)
}
check_tbl = check_tbl %>% mutate(filename = paste0(date2str(fcst_date), '_', run, '_', date2str(init_date), '.grb2'))
check_tbl = check_tbl %>% filter(init_date != fcst_date)
nfiles = nrow(check_tbl)

cl = makeCluster(7)
registerDoParallel(cl)
foreach (i = 1:nfiles) %dopar% {
# for(i in 1:nfiles){
	require(data.table)
	require(dplyr)
	require(lubridate)
	require(stringr)
	require(tidyr)
	file_temp = check_tbl[i, ]$filename
	initdatestr = date2str(check_tbl[i, ]$init_date)
	inityr = substr(initdatestr, 1, 4)
	inityrmon = substr(initdatestr, 1, 6)
	initdatefilestr = substr(initdatestr, 1, 8)
	fcstdatestr = date2str(check_tbl[i, ]$fcst_date)
	runstr_sel = check_tbl[i, ]$run
	url = paste0('http://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf/', inityr, '/', inityrmon, '/', initdatefilestr, '/', initdatestr, '/flxf', fcstdatestr, '.01.', initdatestr, '.grb2')
	if(file.exists(paste0(savdir_gbm, file_temp)) == F){
		try(system(paste0('wget ', url, ' -O ', savdir_scratch, file_temp)))
		if(file.exists(paste0(savdir_scratch, file_temp)) == T){
			system(paste0('wgrib2 ', savdir_scratch, file_temp, ' -match ":(TMP:2 m above ground|PRATE|CPRAT|LHTFL|UGRD:10 m above ground|VGRD:10 m above ground):" -small_grib  73:98 22:32 ',  savdir_gbm, file_temp))		

		}
	}

	if(file.exists(paste0(savdir_africa, file_temp)) == F){
		if(file.exists(paste0(savdir_scratch, file_temp)) == F){
			try(system(paste0('wget ', url, ' -O ', savdir_scratch, file_temp)))
		}
		if(file.exists(paste0(savdir_scratch, file_temp)) == T){
			
			system(paste0('wgrib2 ', savdir_scratch, file_temp, ' -match ":(TMP:2 m above ground|PRATE|CPRAT|LHTFL|UGRD:10 m above ground|VGRD:10 m above ground):" -small_grib  -20:55 -40:40 ',  savdir_africa, file_temp))
		}
	}
	system(paste0('rm ', savdir_scratch, file_temp))
}
stopCluster(cl)
Sys.time() - start_time
