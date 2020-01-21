"""This module contains common functions"""

###############################
# Import Section
###############################

import datetime
import json
import warnings
from io import StringIO
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
import sqlalchemy
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.business import Business
from facebook_business.api import Cursor, FacebookAdsApi
from impala.dbapi import connect

###############################
# AF Section
###############################


class AppsFlyer():

    """
    Class provides connection to AppsFlyer API

    Parameters
    ----------
    app_id: str
        App identificator
    token: str
        API Authorization Key. Account owner API Key should be used

    Methods
    -------
    get_raw_data_report:
        Method provides raw data import from AppsFlyer

    Variables
    ---------
    RAW_DATA_REPORT_TYPES: Dict
        Dictionary with available raw data report types

    Notes
    -----
    AppsFlyer API Reference
    https://support.appsflyer.com/hc/en-us/articles/207034346-Pull-APIs-Pulling-AppsFlyer-Reports-by-APIs#Intro
    """

    _API_ENDPOINT = 'https://hq.appsflyer.com/'
    _API_VERSION = 'v5'
    RAW_DATA_REPORT_TYPES = {'Installations': 'Non-organic Installs',
                             'In-App Events': 'Non-organic In-App Events',
                             'Uninstalls': 'Non-organic Uninstalls',
                             'Organic Installations': 'Organic Installs',
                             'Organic In-App Events': 'Organic In-App Events'}

    def __init__(self, app_id: str, token: str) -> None:

        self._app_id = app_id
        self._token = token

    def _params(self, from_dt: str, to_dt: str, event_name: List[str] = None):

        params = {
            'api_token': self._token,
            'from': from_dt,
            'to': to_dt,
            'timezone': 'Europe/Moscow'
            }

        if event_name is None:
            return params
        else:
            params['event_name'] = ','.join(event_name)
            return params

    def get_raw_data_report(self, from_dt: str, to_dt: str,
                            report_type: str, event_name: List[str] = None,
                            verbose: bool = False):
        """Method provides raw data import from AppsFlyer

        Parameters
        ----------
        from_dt: str
            Start report date, format: "YYYY-MM-DD" e.g. "2019-06-01"
        to_dt: str
            End report date, format: "YYYY-MM-DD" e.g. "2019-06-10"
        report_type: str
            Report type.
            The next types are available:
                * Installations: Non-organic Installs
                * In-App Events: Non-organic In-App Events
                * Uninstalls: Non-organic Uninstalls
                * Organic Installations: Organic Installs
                * Organic In-App Events: Organic In-App Events
        event_name: List, optional
            List of events for report. If event_name is empty, all events will be uploaded.
        verbose: boolean
            Verbose proccess

        Raises
        ------
            HTTPError
            KeyError: If the wrong report type was choosen.
            Warning: If Report limitations limit up to 200K rows reached

        Returns
        -------
        raw_data: pd.DataFrame
            Dataframe with raw data from AppsFlyer

        Examples
        --------
        >>> af = AppsFlyer('app_id', 'token')
        >>> raw_data = af.get_raw_data_report(
                            from_dt='2019-06-01', to_dt='2019-06-24',
                            report_type='In-App Events',
                            event_name=['af_purchase', af_first_purchase])
        """
        _path_dict = {
            'Organic In-App Events': '/organic_in_app_events_report/',
            'In-App Events': '/in_app_events_report/',
            'Installations': '/installs_report/',
            'Uninstalls': '/uninstall_events_report/',
            'Organic Installations': '/organic_installs_report/'
        }

        try:
            url = (self._API_ENDPOINT +
                   'export/' +
                   self._app_id +
                   _path_dict[report_type] +
                   self._API_VERSION)
        except KeyError:
            print("""KeyError:\n
                     You choose wrong report type. Select right one: \n""")
            for k, v in (self.RAW_DATA_REPORT_TYPES.items()):
                print(k + ':\t' + v)
            raise
        else:
            params = self._params(from_dt, to_dt, event_name)
            if verbose:
                print('Request for {0}-{1} sent to AF \n'.format(
                    from_dt, to_dt))

        try:
            r = requests.get(url, params=params)
            if verbose:
                print(r.url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print('HTTPError: {}'.format(errh))
            print(r.content.decode('UTF-8'))
            raise
        else:
            if verbose:
                print('Data received from AF \n')

            raw_data = pd.read_csv(
                StringIO(r.content.decode('utf-8')), low_memory=False)
            if verbose:
                print('Data ready to use \u2705')
                print('Data shape is {}\n'.format(raw_data.shape))

        if len(raw_data) == 200000:
            warnings.warn('Notice. Report limitations: Up to 200K rows. Limit reached.', Warning)

        return raw_data


def transform_events_value(raw_data: pd.DataFrame, events: List, additional_fields: List = None,
                           to_int: List = None, to_float: List = None):
    """Function transforms Event Value from JSON notation to DataFrame columns

    Parameters
    ----------
    raw_data: pd.DataFrame
        DataFrame with raw data from AppsFlyer
    events: List
        List with number of events which value should be transformed
    additional_fields: List, Optional
        List with number of columns that will be in modified dataframe
        For default, there are next columns name will be in final dataframe
            ['AppsFlyer ID', 'Event Time', 'Event Name', 'Event Value']
    """
    fields = ['AppsFlyer ID', 'Event Time', 'Event Name', 'Event Value']

    if additional_fields:
        fields.extend(additional_fields)

    df = raw_data.loc[raw_data['Event Name'].isin(events), fields]
    df = pd.concat([df.drop(['Event Value'], axis=1),
                    pd.DataFrame(
                        df['Event Value'].apply(json.loads).values.tolist(),
                        index=df.index)], axis=1)

    df['Event Time'] = pd.to_datetime(df['Event Time'])
    df['dt'] = df['Event Time'].dt.normalize()

    if to_int:
        df.loc[:, to_int] = df.loc[:, to_int].astype(int)

    if to_float:
        df.loc[:, to_float] = df.loc[:, to_float].astype(float)

    return df.reset_index(drop=True)


def prepare_retention(retention_data: pd.DataFrame):
    """Function prepares retention table"""
    af_retention = (
        retention_data.loc[:, :]
        .sort_values('Date')
        .set_index('Date')
    )

    af_retention_share = round(
       af_retention.loc[:, ~af_retention.columns.isin(['Install Day'])]
       .div(af_retention['Install Day'], axis='index')*100, 2
       )

    af_retention_data = (
        pd.concat([af_retention['Install Day'], af_retention_share], axis=1))

    return af_retention_data


def print_retention(retention_table: pd.DataFrame, dN_retention: int = 7):
    """Function draws retention heatmap and return retention modified dataframe

    Parameters
    ----------
    retention_table: pd.DataFrame
        Retention data from AF
    dN_retention: int, default(7)
        Day of retention
    """

    try:
        ret_af = (
            retention_table
            .loc[:, :'Day {}'.format(dN_retention)]
        )
    except KeyError:
        print('You have chosen a retention value ({}) that is too large'.format(dN_retention))
        print('Maximum retention value will be use')
        ret_af = (
            retention_table.loc[:, :]
        )

    columns = ret_af.columns.str.contains(r'Day \d+')
    ret_af.loc[:, columns] = ret_af.loc[:, columns].astype(str)+'%'

    return ret_af


def plot_retention(retention_table: pd.DataFrame, dN_retention: int = 7):
    """Function makes retention heatmap plot"""

    try:
        ret_af = (
            retention_table
            .loc[:, :'Day {}'.format(dN_retention)]
            .div(100)
        )
    except KeyError:
        print('You have chosen a retention value ({}) that is too large'.format(dN_retention))
        print('Maximum retention value will be use')
        ret_af = (
            retention_table.loc[:, :]
        )

    ret_af.index = [str(i)+'\n size: '+str(s) for i, s in zip(ret_af.index, ret_af['Install Day'])]
    # ret_af = ret_af.loc[:, ~ret_af.columns.isin(['Install Day'])]
    ret_af['Install Day'] = 1.
    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(
            ret_af, cmap='Blues', annot=True,
            fmt='.2f', ax=ax, cbar=False, square=True)
    plt.yticks(rotation=0)
    plt.ylabel('')
    ax.set_title('AF Cohort retention', fontsize=16)
    plt.show()

###############################
# FB Section
###############################


class FaceBookInsights():
    """
    Class provides access to Facebook Insight API

    Parameters
    ----------
    credential: str
        Path to Facebook credential file
    """

    def __init__(self, credential):
        with open(credential) as f:
            self.cred = json.load(f)

        self._my_app_id = self.cred['app']['id']
        self._my_app_secret = self.cred['app']['secret']
        self._my_access_token = self.cred['user']['long_token']
        self._my_business_account = self.cred['ad_account']['business_account']
        self._api = FacebookAdsApi.init(
            self._my_app_id,
            self._my_app_secret,
            self._my_access_token
            )
        self._business = self._set_business_account()
        self._adaccounts = self._set_adaccounts()

    def _set_business_account(self):
        return Business(self._my_business_account, api=self._api)

    def _set_adaccounts(self):
        return self._business.get_owned_ad_accounts(fields=[AdAccount.Field.id, AdAccount.Field.name])

    def get_business_account(self):
        return self._business

    def get_adaccounts(self, business=None):
        return self._adaccounts

    def _params(self, from_dt: str, to_dt: str, kwargs):

        params = {
            'time_range': {
                'since': from_dt,
                'until': to_dt
                }
        }

        if 'level' in kwargs:
            params['level'] = kwargs.get('level')

        return params

    def get_account_insights(self, fb_accounts: Cursor, accounts_id: List[str],
                             from_dt: str, to_dt: str, to_df: bool = True, **kwargs):

        fields = [
            # Ad meta data
            AdsInsights.Field.account_name,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,
            # Ad metrics
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.spend,
            AdsInsights.Field.cpc,
            AdsInsights.Field.ctr,
            AdsInsights.Field.cpp,
            AdsInsights.Field.cpm,
            AdsInsights.Field.reach,
            AdsInsights.Field.frequency,
            AdsInsights.Field.cost_per_action_type,
            AdsInsights.Field.actions,
            AdsInsights.Field.conversion_values,
            AdsInsights.Field.action_values
        ]

        params = self._params(from_dt, to_dt, kwargs)

        insights = []
        for account in fb_accounts:
            if account['id'] in accounts_id:
                insight = account.get_insights(fields=fields, params=params)
                if insight:
                    insight = pd.DataFrame(list(insight))
                    insights.append(insight)
                continue

        return insights


def extract_from_list(actions: List, action_name: str):
    """TODO"""
    if isinstance(actions, list):
        for dic in actions:
            if dic.get('action_type') == action_name:
                return float(dic['value'])


def modify_insight_data(insights: List[pd.DataFrame]):
    """TODO"""

    modified = []
    for insight in insights:
        actions = insight.loc[:, ['actions', 'cost_per_action_type', 'action_values']]
        insight['mobile_app_install'] = actions['actions'].apply(
            lambda actions: extract_from_list(actions, 'mobile_app_install'))
        insight['cost_per_app_install'] = actions['cost_per_action_type'].apply(
            lambda actions: extract_from_list(actions, 'omni_app_install'))
        insight['install_rate'] = (
            insight['mobile_app_install'] / insight['clicks'].astype(int))
        insight['mobile_purchases'] = actions['actions'].apply(
            lambda actions: extract_from_list(actions, 'app_custom_event.fb_mobile_purchase'))
        insight['mobile_revenue'] = actions['action_values'].apply(
            lambda actions: extract_from_list(actions, 'app_custom_event.fb_mobile_purchase'))
        insight['cost_per_purchase'] = actions['cost_per_action_type'].apply(
            lambda actions: extract_from_list(actions, 'omni_purchase'))
        insight.fillna(0, inplace=True)
        insight[['clicks', 'impressions', 'reach', 'mobile_app_install', 'mobile_purchases']] = (
            insight.loc[:, ['clicks', 'impressions', 'reach', 'mobile_app_install', 'mobile_purchases']]
            .astype(int, copy=True))
        insight[['cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'spend', 'mobile_revenue', 'cost_per_purchase']] = (
            insight.loc[:, ['cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'spend', 'mobile_revenue', 'cost_per_purchase']]
            .astype(float, copy=True))
        insight.drop(['actions', 'cost_per_action_type', 'action_values'], axis=1, inplace=True)
        insight.drop(['date_start', 'date_stop'], axis=1, inplace=True)

        modified.append(insight)

    data = pd.concat(modified)

    return data


def prepare_insights_summary(insight_data: pd.DataFrame):
    """Prepare account summary from insights Dataframe"""

    target_columns = [
        'spend', 'impressions', 'clicks',
        'ctr', 'cpc', 'mobile_app_install',
        'cost_per_app_install', 'install_rate']

    summary = insight_data[['campaign_name']+target_columns].set_index('campaign_name')

    # Account Total
    total = summary.sum(axis=0).to_frame().T
    total['ctr'] = total['clicks']/total['impressions']*100
    total['cpc'] = total['spend']/total['clicks']
    total['cost_per_app_install'] = total['spend']/total['mobile_app_install']
    total['install_rate'] = total['mobile_app_install']/total['clicks']
    total.index = ['Account Total']

    total_summary = pd.concat([summary, total])

    return total_summary

###############################
# SQL Section
###############################

# Impala
def conn():
    return connect(host='host', port=00000,
                   auth_mechanism='NOSASL', user='python')

engine = sqlalchemy.create_engine('impala://', creator=conn)

def select(sql: str, **kwargs) -> pd.DataFrame:
    return pd.read_sql(sql, engine, **kwargs)

###############################
# Common Section
###############################


def get_prev_week_dates(weeks=1):
    """Function returns previous week dates

    Parameters
    ----------
    weeks: int
        previous week number

    Examples
    --------
    >>> start_of_week, end_of_week = get_prev_week_dates()
    >>> [start_of_week, end_of_week]
    ['2019-11-18', '2019-11-24']
    """

    today = datetime.datetime.now()
    weekday = today.weekday()
    start_of_week = today - datetime.timedelta(days=weekday, weeks=weeks)
    end_of_week = start_of_week + datetime.timedelta(days=6)

    return start_of_week.strftime('%Y-%m-%d'), end_of_week.strftime('%Y-%m-%d')


def compare_installs(current_week_installs: int, from_dt: str,
                     to_dt: str, previous_week_installs: int = None):

    """Function makes comparison between current week installs and previous

    Parameters
    ----------
    current_week_installs: int
        Number of current week installs
    from_dt: str
        Start date
    to_dt: str
        End date
    previous_week_installs: int, Optional
        Number of previous week installs

    Examples
    --------
    >>> compare_installs(1313, '2019-11-18', '2019-11-24')
    В период с 2019-11-25 по 2019-12-01 выполнено 1313 установок приложения.

    >>> compare_installs(1313, '2019-11-18', '2019-11-24', 3131)
    В период с 2019-11-25 по 2019-12-01 выполнено 1313 установок приложения.
    Это на -58.06% меньше, чем на предыдущей неделе (3131 установок)
    """

    print(
        """В период с {dates[0]} по {dates[1]} выполнено {installs} установок приложения."""
        .format(dates=[from_dt, to_dt], installs=current_week_installs), end=' '
    )

    if previous_week_installs:
        install_share = (current_week_installs/previous_week_installs-1)*100
        if install_share < 0:
            print(
                """Это на {0:.2f}% меньше, чем на предыдущей неделе ({1} установок)"""
                .format(install_share, previous_week_installs)
            )
        else:
            print(
                """Это на {0:.2f}% больше, чем на предыдущей неделе ({1} установок)"""
                .format(install_share, previous_week_installs)
            )
