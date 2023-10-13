import os
import sys
import json
import argparse
import zipfile
import logging
import requests
import tempfile
import jdatetime
import numpy as np
from osgeo import gdal, ogr
from datetime import datetime
from requests.exceptions import HTTPError








class DecTree:

    version = 2023.09
    def __init__(self, address:str, username:str, password:str, input:str, output:str, 
                 landcover:str, false_mask:str, seed_db:bool, **kwargs) -> None:
        
        self.logger = kwargs.get("logger", logging.getLogger("root"))
        self.logger.info(f'======================= This is DecTree v{self.version} ======================')

        self.input_base_dir = input
        self.output_base_dir = output
        self.landcover = landcover
        self.false_mask = false_mask
        
        # datebase connection params
        if seed_db and address and username and password:
            self.url_bin = f'{address}/gcms/api/TreeCoverLossRaster/'
            self.url_nrgb = f'{address}/gcms/api/Sentinel2Raster/'
            auth_token = self.__get_token(address, username , password)
            self.headers = {'Accept': 'application/json', 'Authorization': 'JWT {}'.format(auth_token)}

            self.seed_db = None

            if auth_token:
                try:
                    response = requests.get(self.url_bin, headers=self.headers)
                    # If the response was successful, no Exception will be raised
                    response.raise_for_status()

                    if response.status_code == 200:
                        self.logger.info(f'The database connection was successfully made to the server with IP: {address}.')
                        self.seed_db = True
                    elif response.status_code == 404:
                        self.logger.info('Not Found.')
                    elif response.status_code == 400:
                        self.logger.info('Bad Request.')
                    elif response.status_code == 401:
                        self.logger.info('Unauthorized.')
                    elif response.status_code == 403:
                        self.logger.info('Forbidden.')
                    elif response.status_code == 500:
                        self.logger.info('Internal Server Error.')
                    else:
                        self.logger.info('Unexpected Status Code:', response.status_code)

                except HTTPError as http_err:
                    self.logger.info(f'HTTP error occurred: {http_err}')
                except Exception as err:
                    self.logger.info(f'Other error occurred: {err}')
                else:
                    self.logger.info('Success!')
            else:
                self.logger.info(f'The database connection failed.')

        return None


    @staticmethod
    def init_loggers(msg_level=logging.DEBUG):
        """
        Init a stdout logger
        :param msg_level: Standard msgLevel for both loggers. Default is DEBUG
        """

        logging.getLogger().addHandler(logging.NullHandler())
        # Create default path or get the pathname without the extension, if there is one
        dectree_logger = logging.getLogger("root")
        dectree_logger.handlers = []  # Remove the standard handler again - Bug in logging module

        dectree_logger.setLevel(msg_level)
        formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(message)s")

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        dectree_logger.addHandler(console_handler)
        
        return dectree_logger


    def __getBBox(self, ds):
        """
        Get raster dataset and create bbox for it
        :param ds: gdal dataset
        :return: bbox
        """
        xmin, xpixel, _, ymax, _, ypixel = ds.GetGeoTransform()
        width, height = ds.RasterXSize, ds.RasterYSize
        xmax = xmin + width * xpixel
        ymin = ymax + height * ypixel

        raster_bounds = ogr.Geometry(ogr.wkbLinearRing)
        raster_bounds.AddPoint(xmin, ymin)
        raster_bounds.AddPoint(xmax, ymin)
        raster_bounds.AddPoint(xmax, ymax)
        raster_bounds.AddPoint(xmin, ymax)
        raster_bounds.AddPoint(xmin, ymin)
        bbox = ogr.Geometry(ogr.wkbPolygon)
        bbox.AddGeometry(raster_bounds)

        return bbox


    def __get_token(self, address, username, password):
        token_url = f'{address}/gcms/auth/jwt/create'
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        auth_data = {
            'email': username,
            'password': password
        }
        resp = requests.post(token_url, data=json.dumps(auth_data), headers=headers).json()

        return resp['access'] if 'access' in resp else None
    

    def __process_chmap(self, temp_dir:str, chmap:str, bin_file_path:str):
        lc_ds = gdal.Open(self.landcover, gdal.GA_ReadOnly)
        if lc_ds is None:
            self.logger.info(f'Unable to open {self.landcover}')
            sys.exit(1)
        
        # Get the first input raster band
        lc_band = lc_ds.GetRasterBand(1)
        # Get raster bbox
        lc_bbox = self.__getBBox(lc_ds)
        # The inverse geotransform is used to convert lon/lat degrees to x/y pixel index
        lc_geotrans = lc_ds.GetGeoTransform()
        lc_inv_geotrans = gdal.InvGeoTransform(lc_geotrans)

        # Open input raster by gdal
        fm_ds = gdal.Open(self.false_mask, gdal.GA_ReadOnly)
        if fm_ds is None:
            self.logger.info(f'Unable to open {self.false_mask}')
            sys.exit(1)

        # Get the first input raster band
        fm_band = fm_ds.GetRasterBand(1)

        

        trg_fname = os.path.join(temp_dir, 'CHMAP_3857_temp.tif')
        trg_ds = gdal.Warp(trg_fname, chmap, dstSRS='EPSG:3857', format='GTiff', xRes=10, yRes=10)

        trg_geoTrans = trg_ds.GetGeoTransform()
        self.logger.debug(f'Orginal GeoTransform: {trg_geoTrans}')

        trg_nbands = trg_ds.RasterCount        # Number of bands
        trg_projection = trg_ds.GetProjection()      # Projection

        # Get raster bbox
        trg_bbox = self.__getBBox(trg_ds)

        # Get intersection between two geometry
        intersection = lc_bbox.Intersection(trg_bbox)

        # Check if two geom have intersection
        if intersection is not None and intersection.Area() > 0:

            # Get bound of overlap
            bounds_geo = intersection.Boundary()

            # Get extent of input raster
            xmin_sub, xmax_sub, ymin_sub, ymax_sub = bounds_geo.GetEnvelope()

            # Create a new geomatrix for the image
            new_trg_geoTrans = list(trg_geoTrans)
            new_trg_geoTrans[0] = xmin_sub
            new_trg_geoTrans[3] = ymax_sub
            self.logger.debug(f'New GeoTransform: {trg_geoTrans}')

            # The inverse geotransform is used to convert lon/lat degrees to x/y pixel index
            trg_inv_geotrans = gdal.InvGeoTransform(trg_geoTrans)

            # Convert lon/lat degrees to x/y pixel for the dataset
            ulX_sub, ulY_sub = gdal.ApplyGeoTransform(trg_inv_geotrans, xmin_sub, ymax_sub)
            lrX_sub, lrY_sub = gdal.ApplyGeoTransform(trg_inv_geotrans, xmax_sub, ymin_sub)

            xsize_sub = int(lrX_sub - ulX_sub)
            ysize_sub = int(lrY_sub - ulY_sub)

            # Convert lon/lat degrees to x/y pixel for the dataset
            ulX, ulY = gdal.ApplyGeoTransform(lc_inv_geotrans, xmin_sub, ymax_sub)
            lrX, lrY = gdal.ApplyGeoTransform(lc_inv_geotrans, xmax_sub, ymin_sub)

            xsize = int(lrX - ulX)
            ysize = int(lrY - ulY)

            # Get subset of the raster as a numpy array
            lc_sub_array = lc_band.ReadAsArray(int(ulX), int(ulY), xsize, ysize)
            self.logger.debug(f'Cropped the Landcover image based on tile number.')
            
            # Get subset of the raster as a numpy array
            fm_sub_array = fm_band.ReadAsArray(int(ulX), int(ulY), xsize, ysize)
            mask_fchm = fm_sub_array == 1
            self.logger.debug(f'Cropped the False Mask image based on tile number.')

            image_bands = []
            for b in range(trg_nbands):
                trg_band = trg_ds.GetRasterBand(b + 1)
                image_bands.append(trg_band.ReadAsArray(int(ulX_sub), int(ulY_sub), xsize_sub, ysize_sub))

            blue_band, green_band, red_band, nir_band, kisqr_band = image_bands

            # All changes strong
            total_change_strong = np.logical_and(
                np.logical_and(
                    np.logical_and(blue_band < 10.0, blue_band > 2.0),
                    np.logical_and(red_band < -1.0, red_band > -5.0)),
                np.logical_and(
                    np.logical_and(nir_band < -1.0, nir_band > -5.0),
                    np.logical_and(kisqr_band < 1500, kisqr_band > 150))
            )

            # All changes weak
            total_change_weak = np.logical_and(
                np.logical_and(
                    np.logical_and(blue_band < 11.0, blue_band > 1.0),
                    np.logical_and(red_band < -0.0, red_band > -6.0)),
                np.logical_and(
                    np.logical_and(nir_band < -0.0, nir_band > -6.0),
                    np.logical_and(kisqr_band < 2000, kisqr_band > 100))
            )

            # No data mask
            nodata_mask = kisqr_band >= 2000

            # Mask out other classes
            other_classes = np.isin(lc_sub_array, [2, 3, 4, 5, 6])

            # Mask out unchanged pixels strong
            total_change_strong[other_classes] = 0
            total_change_strong[nodata_mask] = 0

            # Mask out unchanged pixels weak
            total_change_weak[other_classes] = 0
            total_change_weak[nodata_mask] = 0

            sum_change = np.add(total_change_strong, total_change_weak, dtype=int)
            self.logger.debug(f'Sum change image is successfully created.')

            # Write the output into geotiff image.
            sum_fname = os.path.join(temp_dir, 'sum_change_temp.tif')
            drv = gdal.GetDriverByName('GTiff')
            sum_ds = drv.Create(sum_fname, xsize, ysize, 1, gdal.GDT_Byte, options=['COMPRESS=LZW'])
            sum_ds.SetGeoTransform(new_trg_geoTrans)
            sum_ds.SetProjection(trg_projection)
            sum_band = sum_ds.GetRasterBand(1)
            sum_band.WriteArray(sum_change)

            self.logger.debug(f'Sum Change with name {sum_fname} is created.')

            prx_fname = os.path.join(temp_dir, 'proxy_temp.tif')
            prx_ds = drv.Create(prx_fname, xsize, ysize, 1, gdal.GDT_Byte, options=['COMPRESS=LZW'])
            prx_ds.SetGeoTransform(new_trg_geoTrans)
            prx_ds.SetProjection(trg_projection)
            prx_band = prx_ds.GetRasterBand(1)
            self.logger.debug(f'Proxy with name {prx_fname} is created.')

            gdal.ComputeProximity(sum_band, prx_band,
                    options=["VALUES=2", "MAXDIST=5", "DISTUNITS=PIXEL", "NODATA=255", "FIXED_BUF_VAL=0"], callback=None) #gdal.TermProgress

            prx_array = prx_band.ReadAsArray()

            total_change = np.logical_and(total_change_weak, prx_array==0)

            # Forest changes
            forest_changes = np.logical_and(total_change, lc_sub_array==0)

            # Rangeland changes
            rangeland_changes = np.logical_and(total_change, lc_sub_array==1)

            # Assign class labels
            final_array = np.full(lc_sub_array.shape, 255, dtype=int)
            final_array[forest_changes] = 0
            final_array[rangeland_changes] = 1
            
            final_array[mask_fchm] = 255

            driver = gdal.GetDriverByName('GTiff')
            bin_ds = driver.Create(bin_file_path, xsize, ysize, 1, gdal.GDT_Byte, options=['COMPRESS=LZW'])
            bin_ds.SetGeoTransform(new_trg_geoTrans)

            # Create for target raster the same projection as for the value raster
            bin_ds.SetProjection(trg_projection)

            bin_band = bin_ds.GetRasterBand(1)
            bin_band.SetNoDataValue(255)
            bin_band.WriteArray(final_array)

            # Remove cache files
            lc_band.FlushCache()
            fm_band.FlushCache()
            trg_band.FlushCache()
            sum_band.FlushCache()
            prx_band.FlushCache()
            bin_band.FlushCache()

            # Remove temporary files and directory
            trg_ds = None  # Close the wrap GDAL dataset
            sum_ds = None  # Close the temporary GDAL dataset
            prx_ds = None  # Close the proximity dataset
            bin_ds = None  # Close the final binary dataset
        
        return None


    def __db_seeder(self, temp_dir, image_path, ptype):

        base_dir, ext = os.path.splitext(image_path)
        base_dir, fname = os.path.split(base_dir)
        self.logger.debug(f'File path: {image_path}')
        self.logger.debug(f'Split file path into base and file name: {base_dir}, {fname}')

        platform, date_obj, product, tile, c, version, frc, ftype = fname.split('_')
        date_time_str = date_obj.split('-')[0]

        jalili_date =  jdatetime.date.fromgregorian(date=datetime.strptime(date_time_str, '%Y%m%d'))

        yyyymm = jalili_date.strftime("%Y%m")

        pname = ''.join(['_'.join([ptype, yyyymm, tile]), '.tif']) #CHMAP_139802_39SUB.tif

        zfname = ''.join(['_'.join([ptype, yyyymm, tile]), '.zip']) #CHMAP_139802_39SUB.zip

        zf = zipfile.ZipFile(os.path.join(temp_dir, zfname), "w", zipfile.ZIP_DEFLATED)

        zf.write(image_path, pname)
        zf.close()
        self.logger.debug(f'Zipfile created with Id: {zfname}.')

        data = {
            'year': jalili_date.year,
            'month': jalili_date.month,
            'scene_name': tile
        }
        self.logger.debug(data)

        files = {'zip_file': open(os.path.join(temp_dir, zfname), 'rb')}

        if ftype == 'NRGB':
            resp =  requests.post(self.url_nrgb, data=data, headers=self.headers, files=files)
            self.logger.info(resp.text)
        else:
            resp =  requests.post(self.url_bin, data=data, headers=self.headers, files=files)
            self.logger.info(resp.text)
    
        return resp



    def run(self):
        tiles = os.listdir(self.input_base_dir)
        self.logger.info(f'DecTree found these tiles: {tiles}')
        for tile in tiles:
            out_dir = os.path.join(self.output_base_dir, tile)

            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            chmap_paths = os.path.join(self.input_base_dir, tile)
            chmaps = os.listdir(chmap_paths)
            self.logger.info(f'DecTree found these CHMAP images: {chmaps} for this tile: {tiles}')
            for file in chmaps:
                chmap_file_path = os.path.join(chmap_paths, file)

                bname = file.replace('CHMAP', 'BIN')
                bin_file_path = os.path.join(out_dir, bname)

                # Create a temporary directory to store intermediate files
                with tempfile.TemporaryDirectory() as temp_dir:
                    self.logger.info(f'Temporary directory was created at: {temp_dir}')

                    if not os.path.exists(bin_file_path):
                            self.logger.info('Create file %s' % bin_file_path)

                            self.__process_chmap(temp_dir, chmap_file_path, bin_file_path)

                            if self.seed_db:
                                nrgb_name = file.replace('CHMAP', 'NRGB')
                                nrgb_file_path = os.path.join(out_dir.replace('CHMAP', 'L3A'), nrgb_name)
                                
                                self.logger.info(f'DecTree will update database with this NRGB image: {nrgb_name}')
                                self.logger.info(f'DecTree will update database with this BIN map: {bname}')

                                # self.__db_seeder(temp_dir, nrgb_file_path, 'SENTINEL2')
                                self.__db_seeder(temp_dir, bin_file_path, 'CHMAP')

                    else:
                        self.logger.info(f'This file has already been created at: {bin_file_path}')
                        if self.seed_db:
                            self.logger.info(f'The database connection was successfully made to the server with IP: {self.url_bin}')
                            nrgb_name = file.replace('CHMAP', 'NRGB')
                            nrgb_file_path = os.path.join(out_dir.replace('CHMAP', 'L3A'), nrgb_name)
                            
                            self.logger.info(f'DecTree will update database with this NRGB image: {nrgb_name}')
                            self.logger.info(f'DecTree will update database with this BIN map: {bname}')

                            # self.__db_seeder(temp_dir, nrgb_file_path, 'SENTINEL2')
                            self.__db_seeder(temp_dir, bin_file_path, 'CHMAP')

def main():

    parser = argparse.ArgumentParser(description="Perfrom DecTree to extract forest binary change map from iMad outputs.")

    parser.add_argument("-a", "--address", type=str, help="WebApp address")
    parser.add_argument("-u", "--username", type=str, help="WebApp username")
    parser.add_argument("-p", "--password", type=str, help="WebApp password")
    parser.add_argument("-i", "--input", type=str, help="Where did you store L3A products?")
    parser.add_argument("-o", "--output", type=str, help="Where is your preferred direction to store change maps?")
    parser.add_argument("-l", "--landcover", type=str, help="Landcover map")
    parser.add_argument("-m", "--false_mask", type=str, help="False mask map")
    parser.add_argument("-s", "--seed_db", help="Upload output to the WebApp database. Default is fals", 
                        default=False, action="store_true")
    parser.add_argument("-v", "--verbose", help="Provides detailed (DEBUG) logging for DecTree. Default is false",
                        default=False, action="store_true")

    args = parser.parse_args()

    # TODO Add error skipping
    logging_level = logging.DEBUG if args.verbose else logging.INFO
    logger = DecTree.init_loggers(msg_level=logging_level)


    tree = DecTree(args.address, args.username, args.password, args.input, args.output, 
                   args.landcover, args.false_mask, args.seed_db, logger=logger)
    tree.run()
