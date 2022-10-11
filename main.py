from ldap3 import Server, Connection, SUBTREE, LEVEL
#from mysql_class import DB
from datetime import datetime


class AD:
    def __init__(self, server, ad_user, ad_pass):

        self.ad_server = Server(server, port=636, use_ssl=True, get_info='ALL')
        self.connect = Connection(self.ad_server, user=ad_user, password=ad_pass,
                                  fast_decoder=True, auto_bind=True, auto_referrals=True, check_names=False,
                                  read_only=True,
                                  lazy=False, raise_exceptions=False)

        self.locations = ['CP', 'BA', 'CH', 'CZ', 'ES', 'GR', 'HR', 'HU', 'IE', 'IT', 'LT', 'PL', 'PT', 'RO', 'RS', 'TR', 'UK']

        # generic wks and srv OUs
        ou_wks = [f'OU=Workstations,OU={loc},OU=Countries,DC=corporate,DC=affidea,DC=com' for loc in self.locations]
        ou_srv = [f'OU=Servers,OU={loc},OU=Countries,DC=corporate,DC=affidea,DC=com' for loc in self.locations]
        # build the entire ou map, for wks and srv
        self.map_wks = dict(zip(ou_wks, self.locations))
        self.map_srv = dict(zip(ou_srv, self.locations))

    def get_child_ou_dns(self, root_dn):
        results = list()
        elements = self.connect.extend.standard.paged_search(
            search_base=root_dn,
            search_filter='(objectCategory=organizationalUnit)',
            search_scope=LEVEL,
            paged_size=100)
        for element in elements:
            if 'dn' in element:
                if element['dn'] != root_dn:
                    if 'dn' in element:
                        results.append(element['dn'])
        return results

    def get_all_ous(self, root_dn):
        all_ous = dict()
        ou_dn_process_status = dict()
        ou_dn_process_status[root_dn] = {'need_to_process': True}
        has_searches_to_process = True
        while has_searches_to_process:
            ou_dn_process_status_keys = list(ou_dn_process_status.keys())
            for dn in ou_dn_process_status_keys:
                if ou_dn_process_status[dn]['need_to_process']:
                    all_ous[dn] = self.get_child_ou_dns(dn)
                    ou_dn_process_status[dn]['need_to_process'] = False
                    for child_ou_dn in all_ous[dn]:
                        if not child_ou_dn in ou_dn_process_status:
                            ou_dn_process_status[child_ou_dn] = {'need_to_process': True}
            has_searches_to_process = False
            for dn in ou_dn_process_status:
                if ou_dn_process_status[dn]['need_to_process']:
                    has_searches_to_process = True
        return all_ous

    def get_all_ad_hosts(self, root_dn):
        results = []
        elements = self.connect.extend.standard.paged_search(
            search_base=root_dn,
            search_filter='(&(objectCategory=computer)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))',
            search_scope=SUBTREE,
            attributes=['whenCreated', 'operatingSystem',
                        'operatingSystemServicePack', 'name', 'lastLogon',
                        'memberOf', 'whenChanged'],
            paged_size=100)
        for element in elements:
            host = dict()
            if 'dn' in element:
                host['dn'] = element['dn']
                host['name'] = element['attributes'][u'name'][0]
                host['memberOf'] = element['attributes'][u'memberOf']
                # mm
                host['operatingSystem'] = element['attributes'][u'operatingSystem']
                host['lastLogon'] = element['attributes'][u'lastLogon']

                results.append(host)
        return results
        # return element

    @staticmethod
    def filter_group(ou_list, data_list):
        """Compare dn from data_list with default OUs \n
           If matched, add to a dict with key as country
        """
        new_dict = {}
        for ou in ou_list.keys():
            tmp = [e for e in data_list if ou in e['dn']]
            if tmp:
                new_dict[ou_list[ou]] = tmp
        return new_dict

    @staticmethod
    def first2(data_list, locations):
        """Get first 2 letters from 'name' field located in dict
           Compare it with Affidea locations,
           if matched: Return a dict having country as key
        """
        dc = {}
        for l in locations:
            tmp = [d for d in data_list if l in d['name'][0:2]]
            if tmp:
                dc[l] = tmp
        return dc

    @staticmethod
    def dtl_ad(my_dict, now):
        """Dict(Country)-->List + adding current date"""
        new_list = []
        for key, value in my_dict.items():
            for v in value:
                name = v['name']
                dn = v['dn']
                os = v['operatingSystem']
                if os:
                    os = os[0]
                else:
                    os = 'Unknown'
                # new_list.extend([(name, dn, os, key, now)])
                new_list.extend([[name, dn, os, key, now]])
        return new_list

    def connect_db(self):
        db_config = {
            "host": "",
            "user": "",
            "password": "",
            "database": ""
        }
        # add dc data to db
        db = DB(config=db_config)
        return db


if __name__ == "__main__":
    now = datetime.now()
    now = now.strftime('%Y-%m-%d')

    ################ update with credentials ###################################
    ad1 = AD("server", "user", "password")
    ############################################################################
    # get all hosts form DC
    data_ad = ad1.get_all_ad_hosts("OU=Countries,DC=corporate,DC=affidea,DC=com")
    # get DC data
    data_dc = ad1.get_all_ad_hosts('OU=Domain Controllers,DC=corporate,DC=affidea,DC=com')
    # filter for Workstations, Servers and DCs
    ad_wks = ad1.filter_group(ad1.map_wks, data_ad)
    ad_srv = ad1.filter_group(ad1.map_srv, data_ad)
    ad_dc = ad1.first2(data_dc, ad1.locations)

    # from dict to list. getting ready to insert on db
    ad_wks = ad1.dtl_ad(ad_wks, now)
    ad_srv = ad1.dtl_ad(ad_srv, now)
    ad_dc = ad1.dtl_ad(ad_dc, now)

    # all servers from Servers OU
    ad_srv = ad_srv + ad_dc

    # MYSQL staff..
    # db = ad1.connect_db()
    #
    # sql_ad = "INSERT INTO AD_dc (Name, DN, Country, OS, Date) VALUES (%s,%s,%s,%s,%s)"
    # sql_wks = "INSERT INTO AD_wks (Name, DN, OS, Country) VALUES (%s,%s,%s,%s,%s)"
    # sql_srv = "INSERT INTO AD_srv (Name, DN, OS, Country) VALUES (%s,%s,%s,%s,%s)"
    #
    # db.insertmany(sql_wks, ad_wks)
    # db.insertmany(sql_srv, ad_srv)
    # db.insertmany(sql_ad, ad_dc)
    #
    # db.connection.close()
