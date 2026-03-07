#!/usr/bin/env python3
"""
LLM-based Drug Name Cleaning Script

This script uses Qwen2.5-7B-Instruct to clean drug names extracted from FDA FAERS data.
Designed to run on Slurm cluster with GPU support.

Usage:
    python llm_clean_drugs.py --input pediatric_drugs.parquet --output pediatric_drugs_llm_cleaned.parquet
    
Input format:
    - index: int
    - medicinal_product: str (raw drug name)
    
Output format:
    - index: int
    - medicinal_product: str (raw drug name)
    - medicinal_product_llm_clean: str (LLM-cleaned name)
"""

import argparse
import logging
import os
import warnings
from pathlib import Path

import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForCausalLM, AutoTokenizer

warnings.filterwarnings("ignore")
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are AI specialize in drug. Your domain knowledge cover drug name, ingredients, and extract medicine product.
Please try to extract the product name.
Note that you must not give the extra information.
Returning the pure answer is the best way to reponse.
"""

USER_PROMPT_TEMPLATE = """
The given text : {question}
Your task is to extract the product name from the given text.
If it is containing the ingredients, extract them all not only one.
Do not add any extra information that you know, just using the data form the given text.
for example
    - input : AMPHOTERICIN B AND CHOLESTEROL AND DISTEAROYLPHOSPHAT 
    the given input containing 3 medicine so the output should be
    output : [AMPHOTERICIN B, CHOLESTEROL, DISTEAROYLPHOSPHAT]
    
    - input : TYLENOL 8 HOUR EXTENDED RELEASE (ACETAMINOPHEN) GELTABS
    the given input include the product name, the property and also the ingredient.
    you must given the product name only.
    output : [TYLENOL]
    
    -input : UPJOHN (250MG AMPULE) (50MG/ML) ANTITHYMOCYTE GLOBULIN
    the given input include the product name, the property, the amount and also the ingredient.
    you must given the product name only.
    output : [UPJOHN]
    
    - Please make sure that the answer spell correctly before returning the answer
"""


@torch.no_grad()
def inference_batch(model, tokenizer, questions):
    """
    Run batch inference on drug names using LLM.
    
    Args:
        model: HuggingFace model
        tokenizer: HuggingFace tokenizer
        questions: List of drug names to clean
        
    Returns:
        List of cleaned drug names
    """
    batch_messages = []
    for question in questions:
        batch_messages.append([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(question=question)},
        ])

    # Tokenize
    prompts = [
        tokenizer.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
        for messages in batch_messages
    ]
    prompts_tokenize = tokenizer(
        prompts,
        return_tensors="pt",
        padding=True,
        truncation=True
    ).to(model.device)

    # Generate
    outputs = model.generate(
        **prompts_tokenize,
        max_new_tokens=512,
        eos_token_id=tokenizer.eos_token_id,
        do_sample=False,
        temperature=0.6,
    )

    # Decode responses (skip prompt tokens)
    responses = [
        output[len(prompts_tokenize.input_ids[i]):]
        for i, output in enumerate(outputs)
    ]
    decoded = tokenizer.batch_decode(responses, skip_special_tokens=True)
    
    # Clear GPU cache after each batch
    del prompts_tokenize, outputs, responses
    torch.cuda.empty_cache()
    
    return decoded


def main():
    parser = argparse.ArgumentParser(description="LLM-based drug name cleaning")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input parquet file (from S07: index, medicinal_product)"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output parquet file (+ medicinal_product_llm_clean)"
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default="Qwen/Qwen2.5-7B-Instruct",
        help="Path to Qwen model"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2,
        help="Batch size for inference (default=2 for 8GB GPUs)"
    )
    parser.add_argument(
        "--device",
        type=str,
        default="cuda",
        help="Device (cuda/cpu)"
    )
    parser.add_argument(
        "--load-in-8bit",
        action="store_true",
        help="Load model in 8-bit mode (reduces memory by ~50%)"
    )
    
    args = parser.parse_args()
    
    # Validate input
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load data
    logger.info(f"Loading dataset from {input_path}")
    df = pd.read_parquet(input_path)
    
    # Validate columns
    if "medicinal_product" not in df.columns:
        raise ValueError(f"Input must have 'medicinal_product' column. Found: {df.columns.tolist()}")
    
    if "index" not in df.columns:
        logger.warning("No 'index' column found, adding one")
        df = df.reset_index(names=['index'])
    
    logger.info(f"Dataset loaded: {len(df):,} unique drugs")
    
    # Setup device
    device = args.device if torch.cuda.is_available() else 'cpu'
    if device == 'cpu' and args.device == 'cuda':
        logger.warning("CUDA not available, falling back to CPU")
    logger.info(f"Using device: {device}")
    
    # Load model
    logger.info(f"Loading model from {args.model_path}")
    if args.load_in_8bit:
        logger.info("Loading model in 8-bit mode (reduces memory usage)")
    
    tokenizer = AutoTokenizer.from_pretrained(args.model_path)
    
    # Model loading with memory optimization
    # Use "auto" for multi-GPU support (splits model across available GPUs)
    model_kwargs = {
        "device_map": "auto" if device == 'cuda' else device,
        "torch_dtype": torch.float16 if device == 'cuda' else torch.float32,
    }
    
    # Limit memory per GPU to avoid OOM during generation
    if device == 'cuda' and torch.cuda.device_count() > 1:
        # Reserve ~2GB per GPU for generation overhead (KV cache + safety margin)
        # Use 6GB per GPU + CPU offloading for remaining layers
        max_memory = {i: "6GB" for i in range(torch.cuda.device_count())}
        max_memory["cpu"] = "20GB"  # Offload overflow to CPU RAM
        model_kwargs["max_memory"] = max_memory
        model_kwargs["offload_folder"] = "/tmp/offload"  # Disk offloading if needed
        logger.info(f"Setting max_memory: {max_memory}")
    
    if args.load_in_8bit and device == 'cuda':
        model_kwargs["load_in_8bit"] = True
        logger.info("Using 8-bit quantization (requires bitsandbytes)")
    
    model = AutoModelForCausalLM.from_pretrained(
        args.model_path,
        **model_kwargs
    )
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"
    logger.info("Model loaded successfully")
    
    # Run inference in batches
    batch_size = args.batch_size
    cleaned_names = []
    
    logger.info(f"Running inference (batch_size={batch_size})...")
    logger.info(f"Total batches: {(len(df) + batch_size - 1) // batch_size}")
    
    for i in tqdm(range(0, len(df), batch_size), desc="Cleaning drugs"):
        batch = df['medicinal_product'][i:i+batch_size].tolist()
        responses = inference_batch(model, tokenizer, batch)
        cleaned_names.extend(responses)
        
        # Periodic memory cleanup (every 100 batches)
        if (i // batch_size) % 100 == 0:
            torch.cuda.empty_cache()
    
    # Add cleaned column
    df['medicinal_product_llm_clean'] = cleaned_names
    
    # Show samples
    logger.info("\nSample outputs:")
    for i in range(min(5, len(df))):
        logger.info(f"  {i+1}. {df['medicinal_product'].iloc[i]}")
        logger.info(f"     → {df['medicinal_product_llm_clean'].iloc[i]}")
    
    # Save output
    logger.info(f"\nSaving output to {output_path}")
    df.to_parquet(output_path, compression='zstd', index=False)
    
    logger.info(f"✅ Completed! Processed {len(df):,} drugs")
    logger.info(f"   Input:  {input_path}")
    logger.info(f"   Output: {output_path}")
    logger.info(f"   Columns: {df.columns.tolist()}")


if __name__ == "__main__":
    main()

