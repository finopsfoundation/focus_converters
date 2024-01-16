import cProfile
import csv
import functools
import io
import pstats


class Profiler:
    def __init__(self, csv_format=False):
        self.csv_format = csv_format

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Wrap and execute profile
            profiler = cProfile.Profile()
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()
            profiling_result = pstats.Stats(profiler)

            generate_csv_file(args, profiling_result)

            generate_console_output(profiler)

            return result

        def generate_console_output(profiler):
            s = io.StringIO()
            sortby = "cumulative"
            ps = pstats.Stats(profiler, stream=s).sort_stats(sortby)
            ps.print_stats()
            print(s.getvalue())

        def generate_csv_file(args, profiling_result):
            if self.csv_format:
                # Determine the filename based on class and method name
                csv_filename = generate_file_name(args)
                with open(csv_filename, "w", newline="") as f:
                    w = csv.writer(f)
                    # Write the headers
                    headers = [
                        "ncalls",
                        "tottime",
                        "percall",
                        "cumtime",
                        "percall",
                        "filename:lineno(function)",
                    ]
                    w.writerow(headers)

                    # Write each row
                    for row in profiling_result.stats.items():
                        func_name, (cc, nc, tt, ct, callers) = row
                        w.writerow([nc, tt, tt / nc, ct, ct / cc, func_name])

        def generate_file_name(args):
            class_name = args[0].__class__.__name__ if args else "global"
            method_name = func.__name__
            base_filename = f"{class_name}_{method_name}_profile"
            csv_filename = f"{base_filename}.csv"
            return csv_filename

        return wrapper
