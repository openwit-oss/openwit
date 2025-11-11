use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use openwit_indexer::*;
use std::collections::HashMap;
use chrono::Utc;

fn generate_test_document(id: usize) -> Document {
    let mut fields = HashMap::new();
    fields.insert("level".to_string(), FieldValue::String("INFO".to_string()));
    fields.insert("service".to_string(), FieldValue::String(format!("service-{}", id % 10)));
    fields.insert("message".to_string(), FieldValue::String("Test log message with some content".to_string()));
    fields.insert("user_id".to_string(), FieldValue::String(format!("user-{}", id)));
    fields.insert("duration_ms".to_string(), FieldValue::Number((id % 1000) as f64));
    
    Document {
        id: format!("doc-{}", id),
        timestamp: Utc::now(),
        fields,
        raw_size: 256,
    }
}

fn bench_inverted_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("inverted_index");
    
    // Benchmark different cardinality scenarios
    for cardinality in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", cardinality),
            cardinality,
            |b, &card| {
                let mut index = InvertedIndex::new();
                let docs: Vec<_> = (0..*card).map(generate_test_document).collect();
                
                b.iter(|| {
                    for doc in &docs {
                        index.index_single(black_box(doc.clone()));
                    }
                });
            },
        );
    }
    
    // Benchmark query performance
    group.bench_function("query_exact_match", |b| {
        let mut index = InvertedIndex::new();
        let docs: Vec<_> = (0..10000).map(generate_test_document).collect();
        
        // Pre-populate index
        for doc in docs {
            index.index_single(doc);
        }
        
        b.iter(|| {
            let query = Query {
                filters: vec![Filter::Equals {
                    field: "level".to_string(),
                    value: FieldValue::String("INFO".to_string()),
                }],
                ..Default::default()
            };
            index.query(black_box(&query))
        });
    });
    
    group.finish();
}

fn bench_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    
    // Benchmark insertion with different FPP rates
    for fpp in [0.001, 0.01, 0.05].iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", fpp),
            fpp,
            |b, &fpp| {
                let config = BloomConfig {
                    false_positive_rate: *fpp,
                    expected_items: 1_000_000,
                    partitions: 16,
                };
                let mut bloom = BloomIndex::new(config);
                
                b.iter(|| {
                    for i in 0..1000 {
                        let doc = generate_test_document(i);
                        bloom.add(black_box(doc));
                    }
                });
            },
        );
    }
    
    // Benchmark lookups
    group.bench_function("contains_check", |b| {
        let config = BloomConfig::default();
        let mut bloom = BloomIndex::new(config);
        
        // Pre-populate
        for i in 0..100_000 {
            bloom.add(generate_test_document(i));
        }
        
        b.iter(|| {
            bloom.contains(black_box("doc-50000"))
        });
    });
    
    group.finish();
}

fn bench_time_series_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_series_index");
    
    // Benchmark different bucket sizes
    for bucket_size in [3600, 7200, 86400].iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", bucket_size),
            bucket_size,
            |b, &bucket| {
                let mut index = TimeSeriesIndex::new(*bucket);
                
                b.iter(|| {
                    for i in 0..1000 {
                        let doc = generate_test_document(i);
                        index.index_point(black_box(doc));
                    }
                });
            },
        );
    }
    
    // Benchmark range queries
    group.bench_function("range_query", |b| {
        let mut index = TimeSeriesIndex::new(3600);
        
        // Pre-populate with 1 hour of data
        for i in 0..3600 {
            let mut doc = generate_test_document(i);
            doc.timestamp = Utc::now() - chrono::Duration::seconds(3600 - i as i64);
            index.index_point(doc);
        }
        
        b.iter(|| {
            let end = Utc::now();
            let start = end - chrono::Duration::minutes(5);
            index.range_query(black_box(start), black_box(end))
        });
    });
    
    group.finish();
}

fn bench_memory_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    
    // Benchmark document creation overhead
    group.bench_function("document_creation", |b| {
        b.iter(|| {
            generate_test_document(black_box(42))
        });
    });
    
    // Benchmark batch allocation
    group.bench_function("batch_allocation_10k", |b| {
        b.iter(|| {
            let docs: Vec<_> = (0..10_000)
                .map(|i| generate_test_document(black_box(i)))
                .collect();
            black_box(docs);
        });
    });
    
    group.finish();
}

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    
    let test_data = vec![0u8; 1024 * 1024]; // 1MB of data
    
    // LZ4 compression
    group.bench_function("lz4_compress_1mb", |b| {
        b.iter(|| {
            lz4::compress(black_box(&test_data))
        });
    });
    
    // Snappy compression
    group.bench_function("snappy_compress_1mb", |b| {
        b.iter(|| {
            snap::raw::Encoder::new().compress_vec(black_box(&test_data))
        });
    });
    
    // Zstd compression
    group.bench_function("zstd_compress_1mb", |b| {
        b.iter(|| {
            zstd::encode_all(black_box(&test_data[..]), 3)
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_inverted_index,
    bench_bloom_filter,
    bench_time_series_index,
    bench_memory_allocation,
    bench_compression
);
criterion_main!(benches);