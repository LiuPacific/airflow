import geopandas
import matplotlib.pyplot as plt
import sys, os


def visualize(sourceFilePath: str, outputDirPath: str, base_file_name: str, column_name: str):
    # global oklahoma_geometry_info_with_2sfca
    geojson_geometry = geopandas.read_file(sourceFilePath)
    hu_dpi = 300
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.set_axis_off()
    geojson_geometry = geojson_geometry.to_crs(crs='EPSG:4326')
    geojson_geometry.plot(ax=ax, column=column_name, scheme='quantiles', cmap='PuBu', legend=False)
    # .plot(ax=ax, column=column_name, cmap='PuBu', legend=True)
    # .plot(ax=ax, column='Rj', cmap='Reds', legend=True)
    # .plot(ax, color='#4c92C3', alpha = 0.8)
    plt.savefig(r"{}/{}_{}.png".format(outputDirPath, base_file_name, column_name), dpi=hu_dpi,
                bbox_inches='tight', transparent=True)
    # plt.show()
    bbox = geojson_geometry.total_bounds
    lon_left, lat_bottom, lon_right, lat_top = bbox

    print(f"lon_left={lon_left}")
    print(f"lat_bottom={lat_bottom}")
    print(f"lon_right={lon_right}")
    print(f"lat_top={lat_top}")


if __name__ == '__main__':
    print("arguments passed to the script are :", sys.argv)
    if len(sys.argv) < 3:
        print("2 parameters are required: source file path; field to show")
        sys.exit(1)

    # r'{}/{}.geojson'.format(output_dir_path, output_base_filename),
    source_geojson_file_path = sys.argv[1]
    column_name_to_show = sys.argv[2] # Ai

    # output_dir_path = r'/home/typingliu/project/output'
    output_dir_path = '.'
    output_base_filename = 'geojson_image_k'

    # visualize(r'{}/{}.geojson'.format(output_dir_path, output_base_filename), output_dir_path,
    #           output_base_filename, 'Ai')
    visualize(source_geojson_file_path, output_dir_path,
              output_base_filename, column_name_to_show)
