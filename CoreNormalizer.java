package com.progressoft.main;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.progressoft.tools.Normalizer;
import com.progressoft.tools.ScoringSummary;

public class CoreNormalizer implements Normalizer {

	public static final String delimiter = ",";
	private List<String[]> entries;
	private String line;
	private String[] tempArr;
	private List<Integer> dataToNormalize;
	private List<BigDecimal> normalizedData;
	private int columnIndex;
	private BigDecimal variance;
	private BigDecimal median;
	private String[] header;
	private List<Double> numeratorsPart1;

	public CoreNormalizer() {

		entries = new ArrayList<>();
		line = " ";
		dataToNormalize = new ArrayList<>();
		normalizedData = new ArrayList<>();
		columnIndex = 0;
		variance = new BigDecimal(0.00);
		median = new BigDecimal(0.00);
		numeratorsPart1 = new ArrayList<>();
	}

	private BigDecimal getMean(List<Integer> dataToNormalize) {
		int sum = dataToNormalize.stream().reduce(0, Integer::sum);
		BigDecimal f = BigDecimal.valueOf(dataToNormalize.size());
		BigDecimal mean = BigDecimal.valueOf(sum).divide(f, 2, RoundingMode.HALF_EVEN);
//		double mean = BigDecimal.valueOf(sum / f).doubleValue();
		return mean;
	}

	private BigDecimal getStandardDeviation_Variance_Median(List<Integer> dataToNormalize, List<Double> numeratorsPart1) {

		BigDecimal mean = getMean(dataToNormalize);
		for (Integer x : dataToNormalize) {
			BigDecimal partition = BigDecimal.valueOf(x - mean.doubleValue());
			BigDecimal partition2 = BigDecimal.valueOf(partition.pow(2).doubleValue());
			numeratorsPart1.add(partition2.setScale(2, RoundingMode.HALF_EVEN).doubleValue());
		}

		BigDecimal numeratorsPart2 = BigDecimal.valueOf(numeratorsPart1.stream().reduce(0.00, Double::sum));
		variance = BigDecimal.valueOf(numeratorsPart2
				.divide(BigDecimal.valueOf(dataToNormalize.size()), 2, RoundingMode.HALF_EVEN).doubleValue());
		Collections.sort(dataToNormalize);
		int sector2 = (dataToNormalize.size() / 2);
		int sector1 = (dataToNormalize.size() / 2) + 1;
		double numerator = dataToNormalize.get(sector2) + dataToNormalize.get(sector1);
		median = BigDecimal.valueOf(numerator / 2).setScale(2, RoundingMode.DOWN);

		int denumerator = dataToNormalize.size(); // in some references it's : dataToNormalize.size() - 1  !!

		BigDecimal resultStage1 = BigDecimal.valueOf(numeratorsPart2.divide(new BigDecimal(denumerator)).doubleValue());

		BigDecimal standardDeviation = BigDecimal.valueOf(Math.sqrt(resultStage1.doubleValue())).setScale(2,
				RoundingMode.HALF_EVEN);

		return standardDeviation;
	}

	private void stageOne(String colToNormalize, Path csvPath) {
		try {
			InputStream csvInputStream = Files.newInputStream(csvPath);
			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(csvInputStream));

			// get the header , and find the index of the column to normalize
			header = bufferReader.readLine().split(delimiter);
			for (int i = 0; i < header.length; i++) {
				String a = header[i];
				if (a.equalsIgnoreCase(colToNormalize)) {
					columnIndex = i;
				}
			}

			// fill all entries , and get the specific data that should be normalized
			while ((line = bufferReader.readLine()) != null) {
				tempArr = line.split(delimiter);
				entries.add(tempArr);
				dataToNormalize.add(Integer.parseInt(tempArr[columnIndex]));
			}

			bufferReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void lastStage(String newColumn, Path destPath) {

		try {
			// add the new normalized data column , and write in the destination file
			int x = 0;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (String[] row : entries) {
				String fullEntriesAsString = "";

				String[] normalizedRow = new String[row.length + 1];
				String[] updatedHeader = new String[row.length + 1];
				System.arraycopy(row, 0, normalizedRow, 0, row.length);
				System.arraycopy(header, 0, updatedHeader, 0, row.length);

				// if the column to normalize is not the last column -> adjust all columns
				if (columnIndex != row.length - 1) {
					int z = 0;
					for (int j = columnIndex; j < row.length; j++) {
						normalizedRow[row.length + z] = normalizedRow[row.length + z - 1];
						updatedHeader[row.length + z] = updatedHeader[row.length + z - 1];
						z -= 1;
					}
				}

				normalizedRow[columnIndex + 1] = normalizedData.get(x).toString();

				// adjust the new header
				if (x == 0) {
					String head = "";
					updatedHeader[columnIndex + 1] = newColumn;
					for (int v = 0; v < updatedHeader.length - 1; v++) {
						head += updatedHeader[v] + ",";
					}
					head += updatedHeader[updatedHeader.length - 1] + System.lineSeparator();
					fullEntriesAsString += head;
				}

				for (int k = 0; k < normalizedRow.length - 1; k++) {
					fullEntriesAsString += normalizedRow[k] + ",";
				}
				fullEntriesAsString += normalizedRow[normalizedRow.length - 1] + System.lineSeparator();
				baos.write(fullEntriesAsString.getBytes());
				x += 1;
			}

			byte[] bytes = baos.toByteArray();
			destPath = Files.write(destPath, bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public ScoringSummary zscore(Path csvPath, Path destPath, String colToStandardize) {

		stageOne(colToStandardize, csvPath);

		BigDecimal minimum = getMin(dataToNormalize);
		BigDecimal maximum = getMax(dataToNormalize);
		BigDecimal mean = getMean(dataToNormalize);
		BigDecimal standardDeviation = getStandardDeviation_Variance_Median(dataToNormalize, numeratorsPart1);

		for (Integer y : dataToNormalize) {

			normalizedData.add(BigDecimal.valueOf((y - mean.doubleValue()) / standardDeviation.doubleValue())
					.setScale(2, RoundingMode.HALF_EVEN));
		}

		lastStage(colToStandardize + "_z", destPath);

		return new ScoringSummary() {

			@Override
			public BigDecimal variance() {
				return variance;
			}

			@Override
			public BigDecimal standardDeviation() {
				return standardDeviation;
			}

			@Override
			public BigDecimal min() {
				return minimum;
			}

			@Override
			public BigDecimal median() {
				return median;
			}

			@Override
			public BigDecimal mean() {
				return mean;
			}

			@Override
			public BigDecimal max() {
				return maximum;
			}
		};
	}

	@Override
	public ScoringSummary minMaxScaling(Path csvPath, Path destPath, String colToNormalize) {

		stageOne(colToNormalize, csvPath);

		BigDecimal minimum = getMin(dataToNormalize);
		BigDecimal maximum = getMax(dataToNormalize);
		BigDecimal mean = getMean(dataToNormalize);
		BigDecimal standardDeviation = getStandardDeviation_Variance_Median(dataToNormalize, numeratorsPart1);

		// apply the formula of the min-max normalization on each one
		for (Integer variable : dataToNormalize) {
			double p1 = variable - minimum.doubleValue();
			double p2 = maximum.doubleValue() - minimum.doubleValue();
			double result = p1 / p2;
			normalizedData.add(BigDecimal.valueOf(result).setScale(2, RoundingMode.HALF_EVEN));
		}
		String newColumn = colToNormalize + "_mm";
		lastStage(newColumn + "_z", destPath);

		return new ScoringSummary() {

			@Override
			public BigDecimal variance() {
				return variance.setScale(2, RoundingMode.HALF_EVEN);
			}

			@Override
			public BigDecimal standardDeviation() {
				return standardDeviation;
			}

			@Override
			public BigDecimal min() {
				return minimum;
			}

			@Override
			public BigDecimal median() {
				return median;
			}

			@Override
			public BigDecimal mean() {
				return mean;// .setScale(2, RoundingMode.HALF_EVEN).doubleValue();
			}

			@Override
			public BigDecimal max() {
				return maximum;
			}
		};
	}

	public static BigDecimal getMin(List<Integer> list) {
		if (list == null || list.size() == 0) {
			return BigDecimal.valueOf(Integer.MAX_VALUE).setScale(2, RoundingMode.HALF_EVEN);
		}

		return BigDecimal.valueOf(Collections.min(list)).setScale(2, RoundingMode.HALF_EVEN);
	}

	public static BigDecimal getMax(List<Integer> list) {
		if (list == null || list.size() == 0) {
			return BigDecimal.valueOf(Integer.MIN_VALUE).setScale(2, RoundingMode.HALF_EVEN);
		}

		return BigDecimal.valueOf(Collections.max(list)).setScale(2, RoundingMode.HALF_EVEN);
	}

//	public static void main(String[] args) {
//
//		System.out.println("Hello Core");
//
//		try {
//			CoreNormalizer core = new CoreNormalizer();
//
//			Path induction = Files.createTempDirectory("induction");
//			String columnName = "mark";
//			Path csvPath = Paths.get("C:\\jordan\\interview-task-main\\src\\test\\resources\\generated.csv");
//			Path destPath = induction.resolve("generated_scaled.csv");
//
////			ScoringSummary test4MinMax = core.minMaxScaling(csvPath, destPath, "age");
//			ScoringSummary test4zScore = core.zscore(csvPath, destPath, "age");
//			BigDecimal a = test4zScore.variance();
//			System.out.println("why you are reading this ?");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}