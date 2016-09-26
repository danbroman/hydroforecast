###########################################
# analysis_cfsv2.r
# reads in individual 24hr forecast csv files
# combines together
# outputs aggregate values - 5/7/10 day totals
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
library(doParallel)
library(foreach)
library(ggthemes)
library(RColorBrewer)
str2date = function(str_date){
	as.POSIXct(paste0(substr(str_date, 1, 4), '-', substr(str_date, 5, 6), '-', substr(str_date, 7, 8), ' ', substr(str_date, 9, 10), ':00'), tz = 'utc')
}

## user inputs
dir_dom_proc = ''
fileout_dom = '_ethiopia_'

dom_file_list = list.files(dir_dom_proc, pattern = paste0('^.*', fileout_dom, '*.*.csv'))
nfiles = length(dom_file_list)

cl = makeCluster(7)
registerDoParallel(cl)
fcst_ls = foreach(i = 1:nfiles) %dopar% {
	require(data.table)
	require(stringr)
	str2date = function(str_date){
		as.POSIXct(paste0(substr(str_date, 1, 4), '-', substr(str_date, 5, 6), '-', substr(str_date, 7, 8), ' ', substr(str_date, 9, 10), ':00'), tz = 'utc')
	}
	file_temp = dom_file_list[i]
	hour_init_temp = hour(str2date(unlist(str_split(file_temp, pattern = '\\.'))[2]))
	fcst_temp = fread(paste0(dir_dom_proc, file_temp))
	fcst_temp$hour_init = hour_init_temp
	fcst_temp
}
stopCluster(cl)
fcst_dt = do.call('bind_rows', fcst_ls)

fcst_dt = fcst_dt %>% mutate(value = ifelse(value >= 0.01, value, 0), date_fcst = as.Date(date_fcst), date_init = as.Date(date_init)) %>% mutate(fcst_lead = as.numeric(date_fcst - date_init)) %>% filter(date_fcst != date_init, fcst_lead <= 60)

saveRDS(fcst_dt, paste0(dir_dom_proc, 'prcp', fileout_dom, '_24hraccum.rda'))

period_tbl_5day = data.table(fcst_lead = 1:60, period = rep(1:12, each = 5))
period_tbl_7day = data.table(fcst_lead = 1:56, period = rep(1:8, each = 7))
period_tbl_10day = data.table(fcst_lead = 1:60, period = rep(1:6, each = 10))

fcst_dt_5day = fcst_dt %>% left_join(period_tbl_5day) %>% group_by(date_init, hour_init, period, ethbasin, ethbasin_I, name) %>% summarise(value = sum(value)) 

fcst_dt_5day = fcst_dt_5day %>% mutate(date_fcst = date_init + 1 + 5 * period)

saveRDS(fcst_dt_5day, paste0(dir_dom_proc, 'prcp', fileout_dom, '_5dayaccum.rda'))

fcst_dt_7day = fcst_dt %>% left_join(period_tbl_7day) %>% group_by(date_init, hour_init, period, ethbasin, ethbasin_I, name) %>% summarise(value = sum(value)) 

fcst_dt_7day = fcst_dt_7day %>% mutate(date_fcst = date_init + 1 + 7 * period) %>% filter(!is.na(period))

saveRDS(fcst_dt_7day, paste0(dir_dom_proc, 'prcp', fileout_dom, '_7dayaccum.rda'))

fcst_dt_10day = fcst_dt %>% left_join(period_tbl_10day) %>% group_by(date_init, hour_init, period, ethbasin, ethbasin_I, name) %>% summarise(value = sum(value)) 

fcst_dt_10day = fcst_dt_10day %>% mutate(date_fcst = date_init + 1 + 10 * period)

saveRDS(fcst_dt_10day, paste0(dir_dom_proc, 'prcp', fileout_dom, '_10dayaccum.rda'))

