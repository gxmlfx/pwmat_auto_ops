import os
import subprocess
import time
import re
import getpass
import shutil
import argparse
import json

base_dir = '/data/home/wnwn/cf3'
gap_dir = os.path.join(base_dir, 'gap2')

def wait_for_jobs_completion():
    """Wait for all calculation tasks to complete"""
    while True:
        result = subprocess.run(['squeue', '-u', getpass.getuser()], 
                               capture_output=True, text=True)
        # Check if the output has only the title line or is empty
        lines = result.stdout.strip().split('\n')
        print(lines)
        if len(lines) <= 1:
            break
        time.sleep(3)  # Check every 3 seconds

def copy_etot_files(t):
    """Copy the toot.input* file to the target directory, if it is missing, an error will be reported."""
    gap_dir = '/data/home/wnwn/cf3/gap2/'
    files_to_copy = ['etot.input', 'etot.input2', 'etot.input3', 'etot.input4']
    missing_files = []
    for file_name in files_to_copy:
        src = os.path.join(gap_dir, file_name)
        dest = os.path.join(gap_dir, str(t), file_name)
        if os.path.exists(src):
            shutil.copy2(src, dest)
            print(f"Copyed file: {src} -> {dest}")
        else:
            missing_files.append(src)
    if missing_files:
        raise FileNotFoundError(f"The following files were not found:{', '.join(missing_files)}")

def missing_files_error(files):
    missing_files = [f for f in files if not os.path.exists(f)]
    if missing_files:
        raise FileNotFoundError(f"The following files do not exist in the folder {os.getcwd()}: {', '.join(files)}")
    
def step0():       
    """Folder structure"""
    os.chdir(base_dir)
    gap_dir = os.path.join(base_dir, 'gap2')
    dielectric_dir = os.path.join(base_dir, 'dielectric')
    os.chdir(gap_dir)
    print(f"Band gap calculation")
    files = ["1.sh", "etot.input", "etot.input2", "gen.kpt", "sub.sh", "O.SG15.PBE.UPF", "Si.SG15.PBE.UPF"]
    missing_files_error(files)
    print("All required files are present, proceeding with bandgap calculation...")
    print("Running 1.sh script...")

    # Run 1.sh in gap_dir
    subprocess.run(['chmod', '+x', '1.sh'])
    subprocess.run(['bash', '1.sh'])
    # Run 1.sh in dielectric_dir
    os.chdir(base_dir)
    os.chdir(dielectric_dir)
    subprocess.run(['chmod', '+x', '1.sh'])
    subprocess.run(['bash', '1.sh'])
    os.chdir(base_dir)

    # Run rho.sh, copy.sh and rho_dielectric.sh, copy_dielectric.sh
    files = ['rho.sh', 'copy.sh', 'rho_dielectric.sh', 'copy_dielectric.sh']
    missing_files_error(files)
    print('Proceeding with rho.sh script...')
    subprocess.run(['chmod', '+x', 'rho.sh'])
    subprocess.run(['bash', 'rho.sh'])
    print('Proceeding with copy.sh script...')
    subprocess.run(['chmod', '+x', 'copy.sh'])
    subprocess.run(['bash', 'copy.sh'])
    print('Proceeding with rho_dielectric.sh script...')
    subprocess.run(['chmod', '+x', 'rho_dielectric.sh'])
    subprocess.run(['bash', 'rho_dielectric.sh'])
    print('Proceeding with copy_dielectric.sh script...')
    subprocess.run(['chmod', '+x', 'copy_dielectric.sh'])
    subprocess.run(['bash', 'copy_dielectric.sh'])

def step1(t):
    """Bandgap calculation"""
    gap_dir = os.path.join(base_dir, 'gap2')
    target_dir = os.path.join(gap_dir, str(t))
    if not os.path.exists(target_dir):
        raise FileNotFoundError(f"The target directory {target_dir} does not exist.")
    os.chdir(target_dir)
    copy_etot_files(t)
    # os.rename('etot.input1', 'etot.input')
    files = ['etot.input', 'etot.input2']
    missing_files_error(files)
    subprocess.run(['sbatch', 'sub.sh'])
    wait_for_jobs_completion()
    os.rename('etot.input', 'etot.input1')
    os.rename('etot.input2', 'etot.input')
    # shutil.copy(os.path.join(base_dir, 'OUT.VR'), os.path.join(target_dir, 'IN.VR'))
    os.rename('OUT.VR', 'IN.VR')

    print(f"Run split_kp.x gen.kpt...")
    subprocess.run(['split_kp.x', 'gen.kpt'])

    # Submit the task
    print(f"Submitting sbatch sub.sh for time {t}...")
    subprocess.run(['sbatch', 'sub.sh'])
    wait_for_jobs_completion()

    # Run Gap_Read to read the band gap
    print("Run Gap_Read...")
    result = subprocess.run(['Gap_Read'], capture_output=True, text=True)
    print(f"Gap_Read output: {result.stdout}")
    gap_match = re.search(r'Band-gap\s*[=:]?\s*([\d.]+)', result.stdout)
    fermi_match = re.search(r'Fermi level\s*[=:]?\s*([\d.]+)', result.stdout)

    if fermi_match and gap_match:
        band_gap = gap_match.group(1)
        fermi_level = fermi_match.group(1)
        # Save band_gap and fermi_level to JSON
        json_path = os.path.join(target_dir, 'gap_fermi.json')
        with open(json_path, 'w') as f:
            json.dump({'band_gap': band_gap, 'fermi_level': fermi_level}, f, indent=2)
        print(f"Saved band_gap and fermi_level to {json_path}")
        print(f"Band gap at time {t}: {band_gap}\n")
    else:
        print(f"Not matched band gap at time {t}")
    shutil.copy(os.path.join(target_dir, 'REPORT'), os.path.join(target_dir, 'REPORT_NONSCF'))
    os.chdir(base_dir)

def step2(t):
    """Generate IN.OCC and process self-consistent calculation process"""
    os.chdir(base_dir) 

    print(f"Self-consistent calculation of processing time {t}")

    # Generate IN.OCC
    subprocess.run(['generate_tddft_occ.x'], input=str(t), text=True)
    wait_for_jobs_completion()
    
    # Copy to the gap directory. If it does not exist, the folder will be created recursively.
    target_dir = os.path.join('dielectric', str(t))
    # os.makedirs(target_dir, exist_ok=True)
    shutil.copy('IN.OCC', target_dir)
    os.chdir(target_dir)

    # Copy the eto file
    # copy_etot_files(t)
    files = ['etot1.input-3-SCF']
    missing_files_error(files)
    # Self-consistent calculation
    shutil.copy('etot1.input-3-SCF', 'etot.input')
    subprocess.run(['sbatch', 'sub.sh'])
    wait_for_jobs_completion()
    shutil.copy('REPORT', 'REPORT_SCF')

    os.chdir(base_dir)

def step3(t):
    """Handle DOS calculation process"""
    os.chdir(base_dir)
    target_dir = os.path.join('dielectric', str(t))
    os.chdir(target_dir)

    print(f"DOS calculation of processing time {t}")
    # Copy the eto file
    # copy_etot_files(t)
    files = ['OUT.WG', 'etot.input4']
    # DOS calculation
    shutil.copy('OUT.WG', 'IN.WG')

    shutil.copy('etot.input-4-DOS', 'etot.input')
    subprocess.run(['sbatch', 'sub.sh'])
    wait_for_jobs_completion()
    # shutil.copy(os.path.join(target_dir, 'REPORT'), os.path.join(target_dir, 'REPORT_DOS'))
    
    os.chdir(base_dir)

def step4(t):
    """Handle Fermi level reads and RPA calculations"""
    os.chdir(base_dir)
    target_dir = os.path.join('dielectric', str(t))
    # os.chdir(target_dir)
    
    print(f"RPA calculation for processing time {t}")

    # Read Fermi level from gap_fermi.json saved in step0
    json_path = os.path.join(gap_dir, str(t), 'gap_fermi.json')
    if os.path.exists(json_path):
        with open(json_path, 'r') as f:
            data = json.load(f)
        fermi_level = data.get('fermi_level')
        print(f"Loaded Fermi level from gap_fermi.json: {fermi_level}")
        os.chdir(base_dir)
        os.chdir(target_dir)
        # Update RPA.input
        if fermi_level and os.path.exists('RPA.input'):
            with open('RPA.input', 'r') as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if line.strip().startswith('EFERMI'):
                    new_line = re.sub(r'EFERMI\s*=\s*[\d.]+',
                                     f'EFERMI  =    {fermi_level}',
                                     line.strip())
                    lines[i] = new_line + '\n'
                    break
            with open('RPA.input', 'w') as f:
                f.writelines(lines)
            print("Updated Fermi level in RPA.input")
    else:
        print(f"gap_fermi.json not found in {target_dir}")
    
    # Submit RPA calculations
    print("Submit RPA calculations...")
    subprocess.run(['sbatch', 'RPA.sh'])
    wait_for_jobs_completion()
    # shutil.copy(os.path.join(target_dir, 'REPORT'), os.path.join(target_dir, 'REPORT_RPA'))
    
    print(f"The RPA calculation at time {t} is completed")
    os.chdir(base_dir)


def main():
    parser = argparse.ArgumentParser(description='Process different steps of the calculation')
    parser.add_argument('--step', type=int, choices=[0, 1, 2, 3, 4], help='Which step to run (0, 1, 2, 3, or 4)')
    parser.add_argument('--time', type=int, help='Specific time point to process')
    parser.add_argument('--all', action='store_true', help='Process all time points (0-128 step 8)')
    parser.add_argument('--full', action='store_true', help='Run all steps in sequence for all time points')

    args = parser.parse_args()

    if args.full:
        print("Running all steps in sequence for all time points (0-128 step 8)")
        step0(None)
        for t in range(0, 129, 8):
            print(f"\n{'='*50}")
            print(f"Start processing time point: {t} (step 1)")
            print(f"{'='*50}")
            step1(t)
            print(f"\n{'='*50}")
            print(f"Start processing time point: {t} (step 2)")
            print(f"{'='*50}")
            step2(t)
            print(f"\n{'='*50}")
            print(f"Start processing time point: {t} (step 3)")
            print(f"{'='*50}")
            step3(t)
            print(f"\n{'='*50}")
            print(f"Start processing time point: {t} (step 4)")
            print(f"{'='*50}")
            step4(t)
        return

    if args.step is None:
        print("Please specify a step with --step (0, 1, 2, 3, or 4)")
        return

    if args.step == 0:
        print("Running step 0: Fold structure")
        step0()
        return

    # Determine the time to process
    times = []
    if args.time is not None:
        times = [args.time]
    elif args.all:
        times = list(range(0, 129, 8))
    else:
        print("Please specify either --time or --all")
        return

    # Perform the specified steps
    step_func = {1: step1, 2: step2, 3: step3, 4: step4}[args.step]
    for t in times:
        print(f"\n{'='*50}")
        print(f"Start processing time point: {t}")
        print(f"{'='*50}")
        step_func(t)

if __name__ == '__main__':
    main()