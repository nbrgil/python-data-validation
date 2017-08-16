#!/usr/bin/python
import json
import os

import psycopg2


class LeadFlagMetricResult:
    def __init__(self):
        self.conn = None
        with open('./metrics-conf.json') as json_file:
            metrics_conf_json = json.load(json_file)
        self.metric_list = metrics_conf_json['metrics']

    def map_function(self, record):
        """
            Map each record from the list of CNPJ
        :param record: Record from the list
        :return: Dictionary with the metric sum calculated
        """
        metric_sum = 0
        for num, value in enumerate(self.metric_list, start=0):
            metric_sum += self.metric_list[num]['attribute_weight'] * self.metric_list[num]['metric_weight'] * record[
                num + 1]

        return {
            'cnpj_id': record[0],
            'metric': metric_sum
        }

    def db_connect(self):
        conn_string = "host='springfield' port='5431' dbname='slg' user='flyway' password='flyway'"
        print("Connecting to database\n	->%s" % conn_string)
        self.conn = psycopg2.connect(conn_string)

    def run(self):
        result_list = []
        append_list = []
        counter = 0
        self.db_connect()

        file = open("metric-result-no-presumed-revenue.json", "w")

        while append_list is not None:
            counter += 1
            append_list = self.validate_chunk(counter, 10000)
            if append_list is not None:
                json.dump(append_list, file)
                print(counter)

        file.close()
        self.conn.close()

    def validate_chunk(self, page, interval):
        metric_columns = ",".join(map(lambda x: x['expression'], self.metric_list))

        start_lead_id = (page - 1) * interval + 1
        end_lead_id = (page - 1) * interval + interval

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT cnpj_id, %s
            FROM slg.lead_flag
            WHERE lead_id >= %s
            AND lead_id <= %s
            AND version = 0
            AND status = 1
            """
            % (metric_columns, start_lead_id, end_lead_id))

        row = cursor.fetchall()
        cursor.close()

        if len(row) == 0:
            return None
        else:
            return list(map(self.map_function, row))


if __name__ == "__main__":
    obj = LeadFlagMetricResult()
    obj.run()
