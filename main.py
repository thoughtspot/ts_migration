import os
import argparse
from utilities.logger_config import setup_logger, get_logger
from utilities import data_ingest
from twb_parser.metadata_parser import MetadataExtraction
from tableau_ts_migrator.migrator import Migrator

def run_job(input_folder, output_folder, live_flag, operation, configs=None):

    setup_logger(output_folder + "logs") 
    logger = get_logger()
    logger.info(f"Running job with operation '{operation}' on input folder '{input_folder}' and output folder '{output_folder}'")

    # print(flag)

    if not os.path.exists(input_folder):
        logger.error(f"Input folder '{input_folder}' does not exist.")
        return

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)    
        
    if operation == "feasibility" or operation == 'convert':
        logger.info("Running feasibility check")
        parsed_data = MetadataExtraction(input_folder, output_folder,).start_exe(live_flag)
        value = parsed_data.iloc[0]['exec_id']
        logger.info("Feasibility check completed")
        if operation == "feasibility":
            data_ingest.DataIngress().write_dump_data(parsed_data)
            data_ingest.DataIngress().call_procedures(value)
        # Run feasibility check
        if operation == "convert":
            logger.info("Running conversion")
            final_dump = Migrator().migrate(parsed_data, output_folder)
            data_ingest.DataIngress().write_dump_data(final_dump)
            data_ingest.DataIngress().call_procedures(value)
            logger.info("Conversion completed")
        url = f"https://ps-internal.thoughtspot.cloud/?param1=Execution_ID&paramVal1={value}&#/pinboard/caf7e1f5-823a-41ce-9462-bfbd07bd7903"

        logger.info(f"Please check the feasibility report at {url}")
            
        # Define the output file path
        output_file = output_folder + '/combined_output.csv' 
        # Check if the file exists
        if os.path.isfile(output_file):
            # Append data to the existing file without writing the header
            final_dump.to_csv(output_file, mode='a', header=False, index=False)
        else:
            # Write data to a new file including the header
            final_dump.to_csv(output_file, mode='w', header=True, index=False)
    else:
        logger.error(f"Invalid operation '{operation}'.")
        return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run jobs on TWB files.")
    parser.add_argument("operation", type=str, nargs='?', choices=["feasibility", "convert"],default="convert", help="The operation to perform on the files.")
    
    parser.add_argument("input_folder", type=str, help="The folder containing the TWB files.")
    parser.add_argument("output_folder", type=str, help="The folder to store the output files.")

    # making live_flag an optional argument with default value False
    parser.add_argument("--live_flag", type=bool, nargs='?', const=True, default=False, 
                        help="The flag helps to choose what type of DS type is needed live/extract while conversion. Default is False.")

    args = parser.parse_args()
    print("input_folder: ", args.input_folder)
    print("output_folder: ", args.output_folder)
    print("live_flag: ", args.live_flag)
    print("operation: ", args.operation)
    run_job(args.input_folder, args.output_folder, args.live_flag, args.operation)







