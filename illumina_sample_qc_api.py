
import deepdiff
import requests
from pymysql import cursors
from db_connection import *
import os


class illumina_sample_qc:

    def validate_illumina_sample(self):

        json_response, sample_name, lane_run, version = self.run_api()
        sqlresult1, sqlresult2 = self.run_sql_query(sample_name, lane_run, version)

        json_data = []
        json_data1 = []

        for result in sqlresult1:
            json_data.append(dict(result))
        json_data = json.dumps(json_data, use_decimal=True, indent=4)

        for result in sqlresult2:
            json_data1.append(dict(result))
        json_data1 = json.dumps(json_data1, default=str, indent=4)

        with open('sqlresponse1.json', 'w') as json_file:
            json_file.write(json_data)
        with open('sqlresponse2.json', 'w') as json_file:
            json_file.write(json_data1)

        with open('sqlresponse1.json') as f:
            sql_data = json.load(f)
            len_of_sql_response = len(sql_data)
            spec_version = sql_data[0]['specimen_aliquot_run_version']
            lane_run = sql_data[0]['lane_run']
            specimen_aliquot_run_pool_list = [
                {'pool_id': sql_data[0]['pool_id'], 'pool_name': sql_data[0]['pool_name']}]
            specimen_list1_keys = ['clusters_pf', 'reads_pf', 'pct_bases_gt_q30', 'bases_gt_q30', 'ss_received_project',
                                   'pct_perf_barcode', 'pct_one_mismatch_barcode', 'mean_qual_score',
                                   'clusters_perfect_barcode', 'clusters_one_mismatch_barcode', 'pct_of_lane',
                                   'extraction_number', 'repeat_number', 'specimen_name', 'lane_run_version_list']
            specimen_dic1 = {}
            for key in specimen_list1_keys:
                if key == 'pct_of_lane':
                    pct_lane1 = []
                    for i in range(len_of_sql_response):
                        pct_lane = sql_data[i][key]
                        pct_lane1.append(pct_lane)
                    pct_of_lane_value = ', '.join(str(x) for x in pct_lane1)
                if key == 'lane_run_version_list':
                    lane_run_version1 = []
                    for i in range(len_of_sql_response):
                        lane_run_version = sql_data[i]['lane_run_version_list']
                        lane_run_version1.append(lane_run_version)
                    lane_run_version1 = list(set(lane_run_version1))
                    lane_run_version1.sort()
                key1 = key
                value = sql_data[0][key1]
                specimen_dic1[key1] = value
                specimen_dic1.update(specimen_dic1)
            specimen_dic1['pct_of_lane'] = pct_of_lane_value
            specimen_dic1['lane_run_version_list'] = lane_run_version1

            specimen_list2_keys = ['specimen_aliquot_name', 'index_sequence', 'specimen_aliquot_form', 'control',
                                   'tumor_type',
                                   'tumor_blast_pct', 'collection_method', 'indication', 'specimen_source',
                                   'src_tube_barcode',
                                   'ny_origin',
                                   'specimen_aliquot_type', 'na_type', 'assay_code', 'ntc', 'lims_project',
                                   'specimen_aliquot_form_origin']
            specimen_dic2 = {}
            for key in specimen_list2_keys:
                key1 = key
                value = sql_data[0][key1]
                specimen_dic2[key1] = value
                specimen_dic2.update(specimen_dic2)
            control_value = specimen_dic2['control']
            if control_value == "true":
                specimen_dic2['control'] = bool(1)
            else:
                specimen_dic2['control'] = bool(0)
            ntc_value = specimen_dic2['ntc']
            if ntc_value == "true":
                specimen_dic2['ntc'] = bool(1)
            else:
                specimen_dic2['ntc'] = bool(0)
        with open('sqlresponse2.json') as f:
            data = json.load(f)
            lis_order_list_keys = ['lis_order_id', 'lis_order_version', 'sex', 'lp_accession_id', 'lp_order_id',
                                   'lp_patient_id', 'patient']
            pool_dict = {}

            data_len = len(data)
            if data_len == 0:
                with open('sample_qc_sqlquery_results.json') as f:
                    query_merge = json.load(f)
                    specimen_dic = {'specimen_aliquot_run_version': spec_version, 'lane_run': lane_run,
                                    'specimen_aliquot_run_pool_list': specimen_aliquot_run_pool_list}
                    specimen_dic.update(specimen_dic1)
                    specimen_dic.update(specimen_dic2)
                    specimen_dic3 = {'open_lis_order_list': []}
                    specimen_dic.update(specimen_dic3)
                    specimen_aliquot_qc_list = [specimen_dic]
                    query_merge['specimen_aliquot_qc_list'] = specimen_aliquot_qc_list
            else:
                for key in lis_order_list_keys:
                    key1 = key
                    value = data[0][key1]
                    pool_dict[key1] = value
                    pool_dict.update(pool_dict)
                for i in range(len(lis_order_list_keys)):
                    for j in range(data_len):
                        value = lis_order_list_keys[i]
                        del data[j][value]
                    specimen_dic4 = {'open_lis_order_item_list': data}
                    pool_dict.update(specimen_dic4)
                    specimen_dic3 = {'open_lis_order_list': [pool_dict]}
                with open('sample_qc_sqlquery_results.json') as f:
                    query_merge = json.load(f)
                    specimen_dic = {'specimen_aliquot_run_version': spec_version, 'lane_run': lane_run,
                                    'specimen_aliquot_run_pool_list': specimen_aliquot_run_pool_list}
                    specimen_dic.update(specimen_dic1)
                    specimen_dic.update(specimen_dic2)
                    specimen_dic.update(specimen_dic3)
                    specimen_aliquot_qc_list = [specimen_dic]
                    query_merge['specimen_aliquot_qc_list'] = specimen_aliquot_qc_list

        with open('sample_qc_sqlquery_results.json', 'w') as json_file:
            json.dump(query_merge, json_file, indent=4)
        with open('sample_qc_sqlquery_results.json') as f:
            sql_data = json.load(f)
            diff = deepdiff.DeepDiff(json_response, sql_data)
            print("The Illumina Sample QC API was run successfully using ss_sample_name='{}' and lane_run='{}'"
                  .format(sample_name, lane_run, version))
            if diff:
                print("While validating the RESPONSE field and values following differences in the data were found. "
                      "In this report new_value specifies the values from ZION database and old_value defines"
                      " the values from API call")
                print(json.dumps(json.loads(diff.to_json()), indent=4))
            else:
                print("All the RESPONSE field and values were successfully validated")
            os.remove('sqlresponse1.json')
            os.remove('sqlresponse2.json')

    def run_api(self):

        with open('Data_values.json') as f:
            data_value_list = json.load(f)
            api_token = data_value_list[0]['api_token']
            sample_name = data_value_list[0]['ss_sample_name']
            lane_run = data_value_list[0]['lane_run']
            version = data_value_list[0]['version']
            url = "https://sampletracker-test-traversa.sema4.com/api/st/illuminasampleqc?specimen_aliquot_run={},{},{}" \
                .format(sample_name, lane_run, version)
            headers = {'Authorization': '{}'.format(api_token)}

        response = requests.request("GET", url, headers=headers)
        json_response = response.json()
        return json_response, sample_name, lane_run, version

    def run_sql_query(self, sample_name, lane_run, version):

        conn = zion_db_connection().conn_open()
        cursor1 = conn.cursor(cursors.DictCursor)
        cursor2 = conn.cursor(cursors.DictCursor)

        query1 = "SELECT distinct sri.version as specimen_aliquot_run_version,lane_run, \
                srisrp.samplerunpool_id as pool_id, \
                srp.pool_name, sri.clusters_pf, sri.reads_pf, pct_bases_gt_q30,sum(rm.bases_q30) as bases_gt_q30,\
                ss_received_project,round(sri.pct_perf_barcode,2) as pct_perf_barcode,\
                round(sri.pct_one_mismatch,2) as pct_one_mismatch_barcode,\
                round(mean_qual_score,2)as mean_qual_score,sri.mismatch_count_0 as clusters_perfect_barcode,\
                sri.mismatch_count_1 as clusters_one_mismatch_barcode,\
                concat(lane.lane_number,': ', round(sril.pct_of_lane,2)) as pct_of_lane,\
                if(ss_sample_name LIKE 'ISM%',null,substring(ss_sample_name,10,1)) as extraction_number,\
                if(ss_sample_name LIKE 'ISM%',null,substring(ss_sample_name,11,1)) as repeat_number,\
                sample.sample_name as specimen_name,ri.version as lane_run_version_list,ss_sample_name as specimen_aliquot_name,\
                sl.index_sequence, sf.form as specimen_aliquot_form,\
                if(ss_sample_name LIKE 'NTC%' or ss_sample_name LIKE 'PC%','true','false') as control,\
                sample.tumor_type,sample.tumor_pct as tumor_blast_pct,sample.collection_method,\
                sample.indication, sample.sample_source as specimen_source,\
                sample.ci_src_tube_barcode as src_tube_barcode,\
                sample.ny_origin,sl.sample_type as specimen_aliquot_type,\
                sl.na_type,ass.code as assay_code,if(ss_sample_name LIKE 'NTC%','true','false') as ntc,\
                lp.project_code as lims_project, (SELECT sf.form FROM sample_form sf WHERE sf.id =\
                (SELECT slb.sample_form_id FROM sample_lot slb where slb.id=\
                (SELECT sla.parent_sample_lot_id from sample_lot sla where id = sl.parent_sample_lot_id))) as\
                specimen_aliquot_form_origin\
                FROM SampleTrackerDB.sample_run_illumina sri \
                inner join sample_lot sl on sri.sample_lot_id = sl.id \
                inner join sample_form sf on sl.sample_form_id = sf.id \
                inner join sample on sl.sample_id = sample.id\
                inner join sample_run_illumina_sample_run_pools srisrp on sri.id=samplerunillumina_id\
                inner join sample_run_pool srp on srp.id=srisrp.samplerunpool_id\
                inner join read_metric rm on sri.id=rm.sample_run_illumina_id\
                inner join lane on rm.lane_id=lane.id\
                inner join run_illumina ri on ri.id = lane.run_illumina_id\
                inner join sample_run_illumina_lanes sril on sri.id=sril.samplerunillumina_id\
                inner join assay ass on sl.assay_id=ass.id\
                inner join lims_project lp on sample.lims_project_id=lp.id\
                WHERE sri.ss_sample_name='{}' and sri.lane_run='{}' \
                and sri.version='{}' group by lane.lane_number".format(sample_name, lane_run, version)

        query2 = "SELECT DISTINCT lo.order_id as lis_order_id,lo.version as lis_order_version,lo.sex, \
                        lo.lp_accession_id, \
                         lo.lp_order_id, lo.lp_patient_id, lo.patient, lo.version as lis_order_item_version, \
                        loi.lab_number as lab_number,loi.lis_date_initiated as lis_date_initiated,tc.test_code as  \
                        test_code FROM sample_run_illumina sri JOIN sample_lot sl ON sri.sample_lot_id=sl.id \
                        JOIN sample_orders so ON sl.sample_id = so.sample_id \
                        JOIN lis_order lo on so.lisorder_id=lo.id \
                        JOIN lis_order_item_lis_order loilo on lo.id=loilo.lisorder_id \
                        JOIN lis_order_item loi on loilo.lisorderitem_id=loi.id \
                        JOIN test_code tc on loi.test_code_id=tc.id WHERE sri.ss_sample_name='{}' \
                        and sri.lane_run='{}' and sri.version='{}'".format(sample_name, lane_run, version)

        cursor1.execute(query1)
        cursor2.execute(query2)
        sqlresult1 = cursor1.fetchall()
        sqlresult2 = cursor2.fetchall()
        cursor1.close()
        cursor2.close()
        zion_db_connection().conn_close(conn)
        return sqlresult1, sqlresult2


if __name__ == '__main__':
    sampleqc = illumina_sample_qc()
    sampleqc.validate_illumina_sample()
