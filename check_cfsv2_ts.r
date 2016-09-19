###########################################
# get_cfsv2_ncdc.r
# pulls cfsv2 forecasts from NCDC archive
# subsets to gbm and africa domains
# pulls out precip. surface temp, winds, and latent
# heat flux
###########################################

## load libraries
library(stringr)
library(dplyr)
library(data.table)
library(lubridate)
library(tools)

## user inputs
dir_gbm = ''
dir_africa = ''
time_sel_start = as.POSIXct('2011-04-01', tz = 'utc')
time_sel_end = as.POSIXct('2016-09-15 18:00', tz = 'utc')

## gbm domain
# setup
time_list = seq(from = time_sel_start, to = time_sel_end, by = '6 hour')
file_list_gbm = sort(list.files(dir_gbm, pattern = '*grb.'))
nfiles_gbm = length(file_list_gbm)

# checks for local files
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

localfile_dt = file_gbm_dt %>% mutate(year = year(date)) %>% select(file, date) %>% data.table()
# %>% filter(year == 2012) 

completedate_dt = data.table(date = time_list)
missdate_list = left_join(completedate_dt, localfile_dt)
missdate_list = test_fl %>% filter(is.na(file)) %>% mutate(year = year(date))

## africa domain
# setup
time_list = seq(from = time_sel_start, to = time_sel_end, by = '6 hour')
file_list_africa = sort(list.files(dir_africa, pattern = '*grb.'))
nfiles_africa = length(file_list_africa)

# checks for local files
file_africa_dt = data.table()
for(i in 1:nfiles_africa){
	file_temp = file_list_africa[i]
	file_meta = data.table(raw = system(paste('wgrib2', paste0(dir_africa, file_temp), '-ftime'), intern = T)) %>% separate(raw, sep = c(':'), into = c('id', 'ref', 'hour')) %>% separate(hour, sep = ' ', into = c('hour', 'lab1', 'lab2'))
		file_max_hour = max(as.numeric(file_meta$hour))
		file_nfcst = nrow(file_meta)
	file_info = file_path_sans_ext(file_temp)
	file_africa_dt_temp = data.table(file = file_temp, fcst_hour_max = file_max_hour, nfcst_step = file_nfcst, info = file_info) %>% separate(info, sep = '\\.', into = c('var', 'date', 'ensemble')) %>% mutate(date = as.POSIXct(date, format = '%Y%m%d%H', tz = 'utc'))
	file_africa_dt = bind_rows(file_africa_dt, file_africa_dt_temp)
}

localfile_dt = file_africa_dt %>% mutate(year = year(date)) %>% select(file, date) %>% data.table()
# %>% filter(year == 2012) 

completedate_dt = data.table(date = time_list)
missdate_list = left_join(completedate_dt, localfile_dt)
missdate_list = test_fl %>% filter(is.na(file)) %>% mutate(year = year(date))




