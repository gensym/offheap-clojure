package org.gensym.hikesaber.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;

import org.gensym.ohic.benchmark.harness;

public class Records {
    @State(Scope.Benchmark)
    public static class LoadedRecords {
	volatile Object records;

	@Setup(Level.Trial)
	public void prepare() {
	    records = harness.loadRecords();
	}

	@TearDown(Level.Trial)
	public void check() {
	    harness.unloadRecords(records);
	}
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countUniqueBikesTransduce(LoadedRecords records) {
	return harness.countUniqueBikesTransduce(records.records);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countUniqueBikes(LoadedRecords records) {
	return harness.countUniqueBikes(records.records);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countUniqueBikesOffHeap(LoadedRecords records) {
	return harness.countUniqueBikesOffHeap(records.records);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countUniqueBikesOffHeapNth(LoadedRecords records) {
	return harness.countUniqueBikesOffHeapNth(records.records);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countUniqueBikesOffHeapTransduce(LoadedRecords records) {
	return harness.countUniqueBikesOffHeapTransduce(records.records);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRidden(LoadedRecords records) {
	return harness.countMinutesRidden(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenLoop(LoadedRecords records) {
	return harness.countMinutesRiddenLoop(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenTransduce(LoadedRecords records) {
	return harness.countMinutesRiddenTransduce(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenOffHeap(LoadedRecords records) {
	return harness.countMinutesRiddenOffHeap(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenOffHeapNth(LoadedRecords records) {
	return harness.countMinutesRiddenOffHeapNth(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenOffHeapNthKeyword(LoadedRecords records) {
	return harness.countMinutesRiddenOffHeapNthKeyword(records.records, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int countMinutesRiddenOffHeapTransduce(LoadedRecords records) {
	return harness.countMinutesRiddenOffHeapTransduce(records.records, 10);
    }

}
