| phase | app-1 CPU % | app-2 CPU % | Redis CPU % | nginx CPU % | app-1 mem MiB | app-2 mem MiB | Redis used MiB | Redis ops/s | Redis clients | app-1 goroutines | app-2 goroutines |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 75x1-3@3m | 87 | 85 | 96 | 32 | 58 | 58 | 29 | 44782 | 308 | 364 | 373 |
| 0x1@1m | 3 | 3 | 11 | 3 | 57 | 57 | 30 | 584 | 308 | 331 | 331 |
| 375x1-3~10s@2m | 102 | 99 | 104 | 57 | 88 | 89 | 77 | 69277 | 514 | 1711 | 1705 |
| 75x1-3@5m | 90 | 91 | 95 | 33 | 89 | 90 | 102 | 46044 | 514 | 1715 | 1708 |
