#!/bin/bash
#SBATCH --job-name=llm_clean_drugs
#SBATCH --output=logs/llm_clean_%j.[out
#SBATCH --partition=qgpu_gtx1070ti
#SBATCH --gres=gpu:2
#SBATCH --cpus-per-task=12
#SBATCH --mem=60G
#SBATCH --time=7-00:00:00

# LLM Drug Cleaning on Slurm
# 
# Usage:
#   sbatch scripts/slurm_llm_clean.sh pediatric
#   sbatch scripts/slurm_llm_clean.sh adult

set -e

COHORT=${1:-pediatric}
echo "=========================================="
echo "LLM Drug Cleaning: $COHORT"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo "=========================================="

# Paths
PROJECT_ROOT="/share/galaxy/thanyathon/candrug_pipeline"
DATA_DIR="$PROJECT_ROOT/data/staging/s06_split_drug"
MODEL_PATH="/share/galaxy/thanyathon/models"  # Local downloaded model (no internet needed)

INPUT_FILE="$DATA_DIR/${COHORT}_drugs.parquet"
OUTPUT_FILE="$DATA_DIR/${COHORT}_drugs_llm_cleaned.parquet"

# Validate input
if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: Input file not found: $INPUT_FILE"
    echo "Please run Stage 7 first to generate drug list"
    exit 1
fi

# Create logs directory
mkdir -p "$PROJECT_ROOT/logs"

# Activate conda environment
echo ""
echo "Activating conda environment: drug1"

# Initialize conda for bash shell
if [ -f ~/miniforge3/etc/profile.d/conda.sh ]; then
    source ~/miniforge3/etc/profile.d/conda.sh
else
    echo "ERROR: Cannot find conda.sh"
    exit 1
fi

# Now activate the environment
conda activate drug1
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to activate conda environment 'drug1'"
    echo "Available environments:"
    conda env list
    exit 1
fi

# Verify activated environment
echo ""
echo "Active conda environment: $CONDA_DEFAULT_ENV"
conda info --envs | grep "*"

# Check Python and packages
echo ""
echo "Python version:"
python --version
echo ""
echo "Checking packages:"
python -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}')"
python -c "import transformers; print(f'Transformers: {transformers.__version__}')"

# Check GPU
echo ""
echo "GPU Info:"
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv

# Run LLM cleaning
echo ""
echo "Running LLM cleaning..."
echo "  Input:  $INPUT_FILE"
echo "  Output: $OUTPUT_FILE"
echo "  Model:  $MODEL_PATH (local - no internet required)"
echo "  Batch size: 2 (optimized for 2x 8GB GPUs)"
echo ""

# Set memory optimization
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

python "$PROJECT_ROOT/scripts/llm_clean_drugs.py" \
    --input "$INPUT_FILE" \
    --output "$OUTPUT_FILE" \
    --model-path "$MODEL_PATH" \
    --batch-size 2 \
    --device cuda

# Check output
if [ -f "$OUTPUT_FILE" ]; then
    echo ""
    echo "✅ Success! Output file created:"
    ls -lh "$OUTPUT_FILE"
    
    echo ""
    echo "Next steps:"
    echo "1. Verify the output file has correct columns:"
    echo "   python3 -c \"import polars as pl; df = pl.read_parquet('$OUTPUT_FILE'); print('Columns:', df.columns); print('Rows:', df.height)\""
    echo ""
    echo "2. Run Stage 7b to validate:"
    echo "   ./scripts/run_s07b.sh"
else
    echo ""
    echo "❌ ERROR: Output file not created"
    exit 1
fi

echo ""
echo "=========================================="
echo "End time: $(date)"
echo "=========================================="

