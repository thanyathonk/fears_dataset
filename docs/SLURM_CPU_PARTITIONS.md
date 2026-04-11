# SLURM CPU Partitions — สรุปและตัวอย่างการเรียกใช้

อัปเดต: จาก `sinfo` และ `scontrol show`

---

## Partitions ที่ใช้ CPU ได้ (ไม่ใช้ GPU)

| Partition   | Node(s)             | CPUs | RAM (MB) | RAM (GB) | Time limit | หมายเหตุ                    |
|-------------|---------------------|------|----------|----------|------------|-----------------------------|
| **cpu**     | ist-compute-1-001~004 | 80   | 741,069–773,283 | ~722–754 GB | 7 วัน   | Default partition, RAM สูงสุด |
| **bash-cpu**| ist-compute-1-004   | 80   | 773,283  | ~754 GB  | 2 ชั่วโมง | สำหรับทดสอบสั้น ๆ           |
| **scads**   | ist-dgx04           | 80   | 515,875  | ~504 GB  | 7 วัน     | ต้องมี account `scads`      |

---

## รายละเอียดแต่ละ Node

### ist-compute-1-001, 003, 004
- **CPUs**: 80 (ใช้ได้ 1–80 ต่อ job)
- **RAM**: 773,283 MB (~754 GB)
- **Partitions**: cpu, bash-cpu (004), deadline

### ist-compute-1-002
- **CPUs**: 80
- **RAM**: 741,069 MB (~722 GB)

### ist-dgx04 (scads)
- **CPUs**: 80
- **RAM**: 515,875 MB (~504 GB)
- **Partition**: scads (จำกัด account)

---

## ตัวอย่าง sbatch สำหรับ S02

### 1. Partition `cpu` — RAM สูงสุด (แนะนำ)

```bash
#!/bin/bash
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=600G
#SBATCH --time=7-00:00:00
```

- ใช้ได้ CPU 1–80 ต่อ job
- หน่วยความจำสูงสุดต่อ node ~754 GB → `--mem` สูงสุด ~750G ได้
- ใช้ 40 CPUs เพื่อให้ node แบ่งกันได้ 2 jobs

### 2. Partition `cpu` — ใช้ทั้ง node (exclusive)

```bash
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=80
#SBATCH --mem=750G
#SBATCH --time=7-00:00:00
```

- ใช้ครบ 80 CPUs → โอกาสได้ node เฉพาะตัวสูงขึ้น

### 3. Partition `scads` — ถ้ามี account scads

```bash
#SBATCH --partition=scads
#SBATCH --nodes=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=500G
#SBATCH --time=7-00:00:00
```

- RAM สูงสุด ~504 GB → `--mem` ไม่เกิน 500G

### 4. Partition `bash-cpu` — ทดสอบเร็ว

```bash
#SBATCH --partition=bash-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=400G
#SBATCH --time=2:00:00
```

- Time limit เพียง 2 ชั่วโมง ใช้เฉพาะทดสอบ

---

## คู่มือการตั้งค่า

| Parameter        | ช่วงค่าที่ใช้ได้        | คำแนะนำสำหรับ S02          |
|------------------|-------------------------|----------------------------|
| `--cpus-per-task`| 1–80                    | 40 (แบ่ง node) หรือ 80 (exclusive) |
| `--mem`          | ขึ้นกับ node            | 600G (cpu), 500G (scads)   |
| `--time`         | ขึ้นกับ partition       | 7-00:00:00 (cpu, scads)    |

---

## ตรวจสอบทรัพยากรก่อนส่ง job

```bash
# ดู partition และ node
sinfo -o "%P %a %l %D %T %N %c %m"

# ดู node ที่ idle/mixed
sinfo -N -o "%N %P %c %m %t" | grep -E "idle|mix"

# ดูรายละเอียด partition
scontrol show partition cpu
```
