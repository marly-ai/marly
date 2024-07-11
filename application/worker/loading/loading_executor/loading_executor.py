import logging
from application.worker.loading.data_models.loading_response import LoadingResponseModel
from application.integrations.destinations.repositories.destination_repository import DestinationRepository
from application.configuration.repositories.output_mapping_repository import OutputMappingRepository
from application.configuration.repositories.normalization_repository import NormalizationRepository
from application.configuration.configs.normalization_config import NormalizationConfig
import time
import json
import uuid
# t
def generate_config(column_locations, transformed_metrics_dict, output_data_source, output_filename, data_location):
    configs = []
    for column, start_cell in column_locations[data_location].items():
        config = {
            "data_location": data_location,
            "start_cell": start_cell,
            "values": transformed_metrics_dict[column],
            "output_data_source": output_data_source,
            "output_filename": output_filename
        }
        configs.append(config)
    return configs

def run_loading(loading_request):
    start_time = time.time()
    file_name = ""

    transformed_metrics = loading_request.transformed_metrics
    output_data_source = loading_request.output_data_source
    output_data_type = loading_request.output_data_type
    output_filename = loading_request.output_filename
    data_location = loading_request.data_location
    normalization_id = loading_request.normalization_id

    logging.info(f"Loading request received for output_data_type: {output_data_type}")
    logging.info(f"Initializing destination instance for {output_data_source}")
    destination_instance = DestinationRepository.get_integration(output_data_source)

    try:
        if output_filename and normalization_id:
            logging.info(f"Initializing normalization for {output_data_type}")
            output_mapping = OutputMappingRepository.get_by_filename(output_filename)
            if not output_mapping:
                raise ValueError(f"Output mapping not found for filename: {output_filename}")

            logging.info(f"Fetching normalization configuration for id: {normalization_id}")
            normalization_config = NormalizationRepository.get(normalization_id)
            if not normalization_config:
                logging.error(f"Normalization configuration not found for id: {normalization_id}")
                raise ValueError(f"Normalization configuration not found for id: {normalization_id}")
            logging.info(f"Normalization configuration: {normalization_config}")

            logging.info("Getting normalization instance")
            normalization_instance = NormalizationConfig.get_normalization_instance(normalization_config)
            logging.info(f"Normalization instance: {normalization_instance}")
            
            logging.info("Cleaning transformed metrics")
            transformed_metrics_cleaned = transformed_metrics.replace('\n', '').replace('\r', '')
            transformed_metrics_dict = json.loads(transformed_metrics_cleaned)
            logging.info(f"Cleaned transformed metrics: {transformed_metrics_dict}")
            
            logging.info("Generating normalization configs")
            logging.info(f"Column locations: {output_mapping.column_locations}, type: {type(output_mapping.column_locations)}")
            logging.info(f"Transformed metrics dict: {transformed_metrics_dict}, type: {type(transformed_metrics_dict)}")
            logging.info(f"Output data source: {output_data_source}, type: {type(output_data_source)}")
            logging.info(f"Output filename: {output_filename}, type: {type(output_filename)}")
            logging.info(f"Data location: {data_location}, type: {type(data_location)}")
            
            configs = generate_config(
                output_mapping.column_locations,
                transformed_metrics_dict,
                output_data_source,
                output_filename,
                data_location
            )
            normalized_data = normalization_instance.do_normalization(configs)
            
            logging.info(f"Writing normalized data to {output_data_source}")
            file_name = destination_instance.write({
                'file_name': f"{output_filename.rsplit('.', 1)[0]}_{uuid.uuid4().hex[:5]}.xlsx",
                'file_stream': normalized_data
            })
            
            job_status = "SUCCESS" if file_name else "FAILURE"
            logging.info(f"Write operation status: {job_status} to filename: {file_name}")
        elif not normalization_id:
            logging.info("Normalization id not provided, skipping normalization")
            table_name = destination_instance.write({
                'data_location': data_location,
                'transformed_metrics': transformed_metrics
            })
            
            job_status = "SUCCESS" if table_name else "FAILURE"
            logging.info(f"Write operation status: {job_status} to table: {table_name}")
        else:
            job_status = "FAILURE"
            logging.error("Output filename or normalization_id not provided, job failed")


        end_time = time.time()
        total_runtime_seconds = (end_time - start_time)
        if total_runtime_seconds < 60:
            total_runtime = f"{total_runtime_seconds:.2f} seconds"
        else:
            minutes, seconds = divmod(total_runtime_seconds, 60)
            total_runtime = f"{int(minutes)} minutes and {seconds:.2f} seconds"

        logging.info(f"Total runtime: {total_runtime}")

        return LoadingResponseModel(
                loading_result=job_status,
                output_data_source=output_data_source,
                output_data_type=output_data_type,
                time=int(total_runtime_seconds),
                data_location=data_location,
                output_filename=file_name if file_name else None,
                normalization_id=normalization_id
            )
    except Exception as e:
        logging.error(f"Error during loading: {str(e)}")
        return LoadingResponseModel(
            loading_result="FAILURE",
            output_data_source=output_data_source,
            output_data_type=output_data_type,
            time=int(time.time() - start_time),
            data_location=data_location,
            output_filename=None,
            normalization_id=normalization_id
        )