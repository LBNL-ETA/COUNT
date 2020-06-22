from data_processor.Data_Processor import Data_Processor, pd
import os


class Cisco_Processor(Data_Processor):
    def __init__(self, project_path=".", config_file="count_config.yaml", section="data_processor"):
        super.__init__(project_path, config_file, section)

        try:
            cfg = self.config[self.data_processor_section]
            self.ap_mac_address_filename = cfg.get('access_point_mac_address_filename', 'ap_mac_address.csv')
            self.oid = cfg.get('count_oid')
            self.data_folder = 'data'

        except Exception as e:
            self.logger.error(
                "unexpected error while setting configuration from config_file=%s, section=%s, error=%s" % (
                    self.config_file, self.data_processor_section, str(e)))
            raise e

        self._get_ap_mac_address()

    def _get_ap_mac_address(self):
        if not os.path.exists(self.project_path + "/" + self.data_folder + "/" + self.ap_mac_address_filename):
            self.logger.error("cannot find mac address to ap_name map filename=%s" % self.ap_mac_address_filename)
            raise Exception("cannot find mac address to ap_name map filename=%s" % self.ap_mac_address_filename)

        try:
            self.mapping = pd.read_csv(self.project_path + "/" + self.data_folder + "/" + self.ap_mac_address_filename)
            self.mapping.columns = ['ap_mac_address', 'id']
            self.logger.info("successfully retrieved access point mac addresses")
        except Exception as e:
            self.logger.error("cannot read access point mac address file=%s" % self.ap_mac_address_filename)
            raise Exception("cannot read access point mac address file=%s" % self.ap_mac_address_filename)

    def _aggregate(self, df, group_by_col='id', agg_fn='count'):
        df = df.groupby(group_by_col).agg(agg_fn)
        df = df[[df.columns[0]]]
        df.columns = ['count']
        df.index.name = 'ap_name'
        df = df.reset_index()
        return df

    def parse(self, df, column_name='output'):
        df['ap_mac_address'] = df[column_name].str.split('\s+', expand=True)[0].str.rsplit('.', n=3, expand=True)[0]
        df = df.merge(self.mapping, left_on='ap_mac_address', right_on='ap_mac_address', how='inner')
        df = df[['output', 'ap_mac_address', 'id']]
        df = self._aggregate(df, group_by_col='id', agg_fn='count')
        return df

    def process(self, df, ap_name_column='ap_name'):
        df['building'] = df[ap_name_column].str.split('-', expand=True)[1]
        return df

    def filter(self, df):
        return df

    def get_metadata_columns(self):
        return ['ap_name', 'building']

    def get_timeseries_columns(self):
        return ['count']
