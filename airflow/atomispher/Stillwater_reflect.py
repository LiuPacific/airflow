from airflow.spatial_tool import buffer_analysis
from airflow.network_tool import download_tool
if __name__ == '__main__':
    buffer_analysis.do_buffer_analysis()
    # mesot
    medot_url = 'http://www.hao123.com'
    download_tool.downlowd_url(medot_url)

