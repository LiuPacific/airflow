import pandas
import geopandas
import matplotlib.pyplot as plt
import matplotlib
from math import radians, cos, sin, asin, sqrt
import numpy
import networkx

zipcode_accessibility = pandas.core.frame.DataFrame()

oklahoma_zipcode_info = geopandas.geodataframe.GeoDataFrame()
oklahoma_zipcode_population_physician = pandas.core.frame.DataFrame()
# zip tuple hospital_zip-population_zip
oklahoma_zipcode_relation = pandas.core.frame.DataFrame()  # it's a simple version of oklahoma_zipcode_info without geometry field.
oklahoma_zipcode_Rj = pandas.core.frame.DataFrame()
oklahoma_zipcode_Ai = pandas.core.frame.DataFrame()
oklahoma_geometry_info_with_2sfca = geopandas.geodataframe.GeoDataFrame()

# gauss_d0 = 180
# accessibility_file_path = r"/home/typingliu/project\ok_od_travel_3hour.csv"
# output_base_filename = 'gauss_3h'
# #
gauss_d0 = 60
accessibility_file_path = r"/home/typingliu/project/ok_od_travel_3hour.csv"
output_base_filename = 'normal_1hour'


# gauss_d0 = 30
# accessibility_file_path = r"/home/typingliu/project/ok_od_travel_30min.csv"
# output_base_filename = 'gauss_30min'

# gauss_d0 = 0
# accessibility_file_path = r"/home/typingliu/project/ok_od_travel_30min.csv"
# # accessibility_file_path = r"/home/typingliu/project/ok_od_travel_30min_mock.csv"
# output_base_filename = 'normal_30min'

output_dir_path = r'/home/typingliu/project/output'
default_population = 1


def main():
    init_data()
    step1()
    step2()
    output(output_dir_path, output_base_filename)
    # visualize(r'{}\{}.shp'.format(output_dir_path, output_base_filename), output_dir_path, output_base_filename,
    #           'Rj')
    # visualize(r'{}/{}.shp'.format(output_dir_path, output_base_filename), output_dir_path, output_base_filename,
    #           'Ai')
    visualize(r'{}/{}.geojson'.format(output_dir_path, output_base_filename), output_dir_path, output_base_filename,
              'Ai')
    # visualize(r'C:/Users/28793/Documents/temp/20220914/gauss_30min.shp', output_dir_path, output_base_filename,
    #           'Rj')
    # visualize(r'C:/Users/28793/Documents/temp/20220914/gauss_30min.shp', output_dir_path, output_base_filename,
    #           'Ai')


def init_data():
    print("init_data")
    global oklahoma_zipcode_info
    oklahoma_zipcode_info = geopandas.read_file(
        r"/home/typingliu/project/zip_shp/ok_zip_acc.geojson"
    )
    # oklahoma_zipcode_info.drop(oklahoma_zipcode_info[oklahoma_zipcode_info['population'].isnull()].index, inplace=True,
    #                            axis=0)
    # oklahoma_zipcode_info.drop(oklahoma_zipcode_info[oklahoma_zipcode_info['physician'].isnull()].index, inplace=True,
    #                            axis=0)
    oklahoma_zipcode_info.fillna(0, inplace=True)
    oklahoma_zipcode_info = oklahoma_zipcode_info.astype({'zip': str, 'population': int})

    global oklahoma_zipcode_population_physician
    oklahoma_zipcode_population_physician = oklahoma_zipcode_info[['zip', 'population', 'physician']]

    oklahoma_zipcode_info.loc[oklahoma_zipcode_info['population'] == 0, 'population'] = default_population

    global oklahoma_zipcode_relation
    oklahoma_zipcode_relation = oklahoma_zipcode_info[['zip', 'physician', 'population']]

    global zipcode_accessibility
    zipcode_accessibility = pandas.read_csv(accessibility_file_path)
    # zipcode_accessibility = pandas.read_csv(r"C:/typing/data/research/projects/okla/okl_data/ok_od_travel_3hour.csv")
    zipcode_accessibility = zipcode_accessibility.astype({'OZCTA': str, 'DZCTA': str})
    # weed_accessibility()
    # zipcode_accessibility.groupby(by='OZCTA').count().to_csv
    # zipcode_accessibility.groupby(by='OZCTA').count().reset_index().to_csv(r'C:/Users/28793/Documents/temp/20220914/accessiblity_quality.csv')

    init_relation()


# pick one-way route out
def weed_accessibility():
    global zipcode_accessibility
    zipcode_accessibility = zipcode_accessibility.assign(one_way='NaN')

    for i in range(len(zipcode_accessibility)):
        if zipcode_accessibility['one_way'][i] == 'NaN':
            # dzcta = zipcode_accessibility['DZCTA'][i]
            # ozcta = zipcode_accessibility['OZCTA'][i]
            zip_result = zipcode_accessibility.loc[
                (zipcode_accessibility['OZCTA'] == zipcode_accessibility['DZCTA'][i]) &
                (zipcode_accessibility['DZCTA'] == zipcode_accessibility['OZCTA'][i])
                ]
            # print(zip_result)
            if len(zip_result) == 0:
                print("{}, OZCTA: {}, DZCTA: {} is one way".format(i, zipcode_accessibility['OZCTA'][i],
                                                                   zipcode_accessibility['DZCTA'][i]))
                zipcode_accessibility.at[i, 'one_way'] = 1

            elif len(zip_result) == 1:
                zipcode_accessibility.at[i, 'one_way'] = 0
                zipcode_accessibility.at[zip_result.index[0], 'one_way'] = 0
            else:
                print('len(result) > 1')
    zipcode_accessibility.drop(zipcode_accessibility.loc[zipcode_accessibility['one_way'] == 1].index, inplace=True)


# init_relation
# return
#      zip  physician population  OZCTA  DZCTA  EstTime  EstDist d_population
# 0  73002          0       1150  73002  73002    8.591    3.815         1150
# 1  73003         67      23406  73003  73003    2.949    1.282        23406
# 2  73003         67      23406  73012  73003   23.955   11.895        23406
# 3  73003         67      23406  73013  73003   22.117   11.026        23406
# 4  73003         67      23406  73025  73003   27.737   14.792        23406
def init_relation():
    print("init_relation")
    global oklahoma_zipcode_relation
    # 30min population
    # using merge instead of calculation one by one
    print(len(oklahoma_zipcode_relation))  # 2824
    oklahoma_zipcode_relation = oklahoma_zipcode_relation.merge(
        zipcode_accessibility, left_on='zip', right_on='OZCTA', how='left'
    )

    oklahoma_zipcode_relation = oklahoma_zipcode_relation.merge(
        oklahoma_zipcode_population_physician, left_on='DZCTA', right_on='zip', how='left')
    print("--")
    oklahoma_zipcode_relation.rename(
        columns={'zip_x': 'zip', 'population_x': 'population', 'population_y': 'd_population', 'physician_x':'physician', 'physician_y':'d_physician1000'},
        inplace=True)
    oklahoma_zipcode_relation['d_physician1000'] = oklahoma_zipcode_relation['d_physician1000'].apply(phy_10000)
    oklahoma_zipcode_relation.drop('zip_y', axis=1, inplace=True)

    # oklahoma_zipcode_relation['d_population'] = oklahoma_zipcode_relation['DZCTA'].apply(getZipPopulation)


def phy_10000(num: int) -> int:
    return num * 10000


# def getZipcodeAccessibility(ori_zipcode: str) -> pandas.core.frame.DataFrame:
#     zip = zipcode_accessibility[zipcode_accessibility['OZCTA'] == ori_zipcode]
#     return zip


# def getZipPopulation(zipcode: str) -> int:
#     population_field = oklahoma_zipcode_info[
#         oklahoma_zipcode_info['zip'] == zipcode
#         ]['population']
#     if len(population_field.values) > 0 and population_field.values[0] != None:
#         return population_field.values[0]
#     else:
#         print("population_field.values[0] is None, zipcode:{}, population:{}", zipcode, population_field)
#         return 0
#         # print("population_field.values[0] is None, zipcode:{}, population:{}", zipcode,
#         #       population_field.values[0])
#         # return 0
#
#     # supply capability


def step1() -> pandas.core.frame.DataFrame:
    print("step1()")
    global oklahoma_zipcode_Rj
    global oklahoma_zipcode_relation
    # direction from patients to hospital or from hospital to patients
    # here I groupby DZCTA, meaning from patients to hospitals.
    oklahoma_zipcode_Rj = oklahoma_zipcode_relation \
        .groupby(by='DZCTA') \
        .apply(get_Rj) \
        .reset_index()
    oklahoma_zipcode_Rj.columns = ['zip', 'Rj']
    oklahoma_zipcode_relation = oklahoma_zipcode_relation \
        .merge(oklahoma_zipcode_Rj, left_on='DZCTA', right_on='zip', how='left')

    oklahoma_zipcode_relation.rename(columns={'zip_x': 'zip'}, inplace=True)
    oklahoma_zipcode_relation.rename(columns={'Rj': 'd_Rj'}, inplace=True)
    oklahoma_zipcode_relation.drop('zip_y', axis=1, inplace=True)

    # add zip_Rj
    # oklahoma_zipcode_relation = oklahoma_zipcode_relation \
    #     .merge(oklahoma_zipcode_Rj, left_on='zip', right_on='zip', how='left')
    # oklahoma_zipcode_relation.rename(columns={'Rj': 'd_Rj'}, inplace=True)

    print(len(oklahoma_zipcode_relation))


def step2() -> pandas.core.frame.DataFrame:
    print("step2()")
    global oklahoma_zipcode_Ai
    global oklahoma_zipcode_Rj
    global oklahoma_zipcode_relation

    # oklahoma_zipcode_relation1 = oklahoma_zipcode_relation[['zip', 'Rj', 'EstTime']]
    # groupby zip with d_Rj = from patients to hospitals
    # groupby DZCTA with zip_Rj = from hospitals to patients
    oklahoma_zipcode_Ai = oklahoma_zipcode_relation \
        .groupby('zip') \
        .apply(get_Ai) \
        .reset_index()

    oklahoma_zipcode_Ai.columns = ['zip', 'Ai']
    print("oklahoma_zipcode_Ai")
    oklahoma_zipcode_relation = oklahoma_zipcode_relation \
        .merge(oklahoma_zipcode_Ai, left_on='zip', right_on='zip', how='left')

    print(len(oklahoma_zipcode_relation))


# for zip_info_index in range(len(oklahoma_zipcode_supply)):
#     zipNear = getZipcodeAccessibility(oklahoma_zipcode_supply.iloc[zip_info_index].at['zip'])
#     population = 0
#     # for i in range(len(zipNear)):
#     #     nearZip = zipNear.iloc[i].at['DZCTA']
#     #     nearZipPopulation = oklahoma_zipcode_supply[
#     #         oklahoma_zipcode_supply['zip'] == str(int(nearZip))
#     #         ]['population']
#     #     if len(nearZipPopulation.values) == 1 and nearZipPopulation.values[0] != None:
#     #         print("{} : {}".format(nearZip, nearZipPopulation.values[0]))
#     #         population = population + int(nearZipPopulation.values[0])
#     #     else:
#     #         print("len(nearZipPopulation.values) != 1, zipcode:{}, population:{}", nearZip, nearZipPopulation)
#     # oklahoma_zipcode_supply.apply()
#     # population_30min
#     oklahoma_zipcode_supply.at[zip_info_index, 'population_30min'] = population
#
# return oklahoma_zipcode_supply

def get_Gaussian(x):
    if gauss_d0 == 0:
        return 1

    if x <= gauss_d0:
        G = (numpy.exp(-0.5 * (x / gauss_d0) ** 2) - numpy.exp(-0.5)) / (1 - numpy.exp(-0.5))
        return G
    else:
        return 0


def get_Rj(x: pandas.core.frame.DataFrame):
    x = x.reset_index()
    Sj = x['d_physician1000'][0]
    dt = 0
    for i in range(len(x)):
        vl = x['d_population'][i] * get_Gaussian(x['EstTime'][i])
        dt += vl
    if Sj == 0:
        return 0
    if dt == 0:
        return None
    return Sj / dt


def get_Ai(x: pandas.core.frame.DataFrame):
    x = x.reset_index()
    dt = 0
    for i in range(len(x)):
        if x['d_Rj'][i] == None:
            print("vl none1")
        vl = x['d_Rj'][i] * get_Gaussian(x['EstTime'][i])
        # vl = x['d_Rj'][i]
        if vl == None:
            print("vl none")
            vl = 0
        dt += vl
    return dt


def output(path: str, file_name: str):
    print("output()")
    global oklahoma_geometry_info_with_2sfca
    oklahoma_geometry_info_with_2sfca = oklahoma_zipcode_info.merge(
        oklahoma_zipcode_Rj, left_on='zip', right_on='zip', how='left')
    oklahoma_geometry_info_with_2sfca = oklahoma_geometry_info_with_2sfca.merge(
        oklahoma_zipcode_Ai, left_on='zip', right_on='zip', how='left')
    print("oklahoma_zipcode_info_with_2sfca:{}".format(len(oklahoma_geometry_info_with_2sfca)))
    print("oklahoma_zipcode_info:{}".format(len(oklahoma_zipcode_info)))

    # oklahoma_zipcode_info_with_2sfca.to_file(r'C:\Users\28793\Documents\temp\a.geojson', driver='GeoJSON')

    # file_name = 'gauss_3hour'
    oklahoma_zipcode_Ai.to_csv(
        path + '/{}_Ai.csv'.format(file_name))
    oklahoma_geometry_info_with_2sfca.to_file(
        path + '/{}.geojson'.format(file_name), driver="GeoJSON")
    # oklahoma_geometry_info_with_2sfca.to_file(
    #     path + r'/{}.shp'.format(file_name))
    # oklahoma_geometry_info_with_2sfca[['zip', 'physician', 'population', 'Rj', 'Ai']].to_csv(
    #     path + r'\{}.csv'.format(file_name))
    # visualize(path + r'\{}_Rj.png'.format(file_name), 'Rj')
    # visualize(path + r'\{}_Ai.png'.format(file_name), 'Ai')


def visualize(sourceFilePath: str, outputDirPath: str, base_file_name: str, column_name: str):
    # global oklahoma_geometry_info_with_2sfca
    oklahoma_geometry_info_with_2sfca = geopandas.read_file(sourceFilePath)
    fig, ax = plt.subplots(figsize=(10, 10))
    oklahoma_geometry_info_with_2sfca \
        .to_crs(crs='EPSG:4326') \
        .plot(ax=ax, column=column_name, scheme='quantiles', cmap='PuBu', legend=True)
    # .plot(ax=ax, column=column_name, cmap='PuBu', legend=True)
    # .plot(ax=ax, column='Rj', cmap='Reds', legend=True)
    # .plot(ax, color='#4c92C3', alpha = 0.8)
    plt.savefig(r"{}/{}_{}.png".format(outputDirPath, base_file_name, column_name))
    plt.show()


if __name__ == '__main__':
    main()
    # output(r'C:\Users\28793\Documents\temp\20220914')
