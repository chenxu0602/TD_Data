use csv::{ReaderBuilder};
use std::error::Error;
use std::fs::{self, File};
use polars::prelude::*;
use std::env;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <folder_name>", args[0]);
        std::process::exit(1);
    }

    let folder_name = &args[1];

    let folder_path = Path::new(folder_name);

    let output_dir = Path::new("processed_data");
    fs::create_dir_all(output_dir)?;

    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) != Some("csv") {
            continue;
        }

        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("output.csv");

        let output_file_path = output_dir.join(file_name);

        println!("{:>50}      -->      {:<50}", path.display(), output_file_path.display());

        let input_file = File::open(&path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(input_file);

        let mut ids         :   Vec<u64>  = Vec::new();
        let mut prices      :   Vec<f64>  = Vec::new();
        let mut amounts     :   Vec<f64>  = Vec::new();
        let mut sides       :   Vec<i32>   = Vec::new();
        let mut packets     :   Vec<i64> = Vec::new();
        let mut receveds    :   Vec<i64> = Vec::new();

        let mut bid0_prices :   Vec<f64>  = Vec::new();
        let mut bid1_prices :   Vec<f64>  = Vec::new();
        let mut bid2_prices :   Vec<f64>  = Vec::new();
        let mut bid3_prices :   Vec<f64>  = Vec::new();
        let mut bid4_prices :   Vec<f64>  = Vec::new();
        let mut bid5_prices :   Vec<f64>  = Vec::new();
        let mut bid6_prices :   Vec<f64>  = Vec::new();
        let mut bid7_prices :   Vec<f64>  = Vec::new();
        let mut bid8_prices :   Vec<f64>  = Vec::new();
        let mut bid9_prices :   Vec<f64>  = Vec::new();

        let mut ask0_prices :   Vec<f64>  = Vec::new();
        let mut ask1_prices :   Vec<f64>  = Vec::new();
        let mut ask2_prices :   Vec<f64>  = Vec::new();
        let mut ask3_prices :   Vec<f64>  = Vec::new();
        let mut ask4_prices :   Vec<f64>  = Vec::new();
        let mut ask5_prices :   Vec<f64>  = Vec::new();
        let mut ask6_prices :   Vec<f64>  = Vec::new();
        let mut ask7_prices :   Vec<f64>  = Vec::new();
        let mut ask8_prices :   Vec<f64>  = Vec::new();
        let mut ask9_prices :   Vec<f64>  = Vec::new();

        let mut bid0_sizes  :  Vec<f64>  = Vec::new();
        let mut bid1_sizes  :  Vec<f64>  = Vec::new();
        let mut bid2_sizes  :  Vec<f64>  = Vec::new();
        let mut bid3_sizes  :  Vec<f64>  = Vec::new();
        let mut bid4_sizes  :  Vec<f64>  = Vec::new();
        let mut bid5_sizes  :  Vec<f64>  = Vec::new();
        let mut bid6_sizes  :  Vec<f64>  = Vec::new();
        let mut bid7_sizes  :  Vec<f64>  = Vec::new();
        let mut bid8_sizes  :  Vec<f64>  = Vec::new();
        let mut bid9_sizes  :  Vec<f64>  = Vec::new();

        let mut ask0_sizes  :  Vec<f64>  = Vec::new();
        let mut ask1_sizes  :  Vec<f64>  = Vec::new();
        let mut ask2_sizes  :  Vec<f64>  = Vec::new();
        let mut ask3_sizes  :  Vec<f64>  = Vec::new();
        let mut ask4_sizes  :  Vec<f64>  = Vec::new();
        let mut ask5_sizes  :  Vec<f64>  = Vec::new();
        let mut ask6_sizes  :  Vec<f64>  = Vec::new();
        let mut ask7_sizes  :  Vec<f64>  = Vec::new();
        let mut ask8_sizes  :  Vec<f64>  = Vec::new();
        let mut ask9_sizes  :  Vec<f64>  = Vec::new();

        let mut order_sizes :  Vec<f64>  = Vec::new();
        let mut tot_amts    :  Vec<f64>  = Vec::new();

        let n_cols: usize = 48;

        let mut order_size: f64 = 0.;
        let mut tot_amt: f64 = 0.;

        let mut prev_fields: Option<Vec<String>> = None;

        // Iterate over each record
        for (idx, result) in reader.records().enumerate() {

            let record = result?;
            let record_str = record.as_slice();

            let fields: Vec<String> = record_str.split('\t').map(|s| s.trim().to_string()).collect();

            if fields.len() != n_cols {
                println!("Wrong num of fields: {}", fields.len());
                continue;
            }

            let mut skip: bool = true;

            if let Some(prev) = &prev_fields {
                if idx > 0 {
                    for i in 6..=46 {
                        if prev[i] != fields[i] {
                            skip = false; 
                            break;
                        } 
                    }
                }
            }

            let amt = fields[2].parse().unwrap_or(0.); 
            let prc = fields[1].parse().unwrap_or(0.); 

            order_size += amt;
            tot_amt += amt * prc;

            if skip { 
                prev_fields = Some(fields);
                continue; 
            } else {
                order_sizes.push(order_size);
                tot_amts.push(tot_amt);

                order_size = 0.;
                tot_amt = 0.;
            }

            let id:           u64  = fields[0].parse().unwrap_or(0);
            let price:        f64  = fields[1].parse().unwrap_or(0.0);
            let amount:       f64  = fields[2].parse().unwrap_or(0.0);
            let side:         i32  = fields[3].parse().unwrap_or(0);
            let packet:       i64  = fields[4].parse().unwrap_or(0);
            let receved:      i64  = fields[5].parse().unwrap_or(0);

            let ask0_size:    f64  = fields[6].parse().unwrap_or(0.0);
            let ask0_price:   f64  = fields[7].parse().unwrap_or(0.0);
            let bid0_size:    f64  = fields[8].parse().unwrap_or(0.0);
            let bid0_price:   f64  = fields[9].parse().unwrap_or(0.0);

            let ask1_size:    f64  = fields[10].parse().unwrap_or(0.0);
            let ask1_price:   f64  = fields[11].parse().unwrap_or(0.0);
            let bid1_size:    f64  = fields[12].parse().unwrap_or(0.0);
            let bid1_price:   f64  = fields[13].parse().unwrap_or(0.0);

            let ask2_size:    f64  = fields[14].parse().unwrap_or(0.0);
            let ask2_price:   f64  = fields[15].parse().unwrap_or(0.0);
            let bid2_size:    f64  = fields[16].parse().unwrap_or(0.0);
            let bid2_price:   f64  = fields[17].parse().unwrap_or(0.0);

            let ask3_size:    f64  = fields[18].parse().unwrap_or(0.0);
            let ask3_price:   f64  = fields[19].parse().unwrap_or(0.0);
            let bid3_size:    f64  = fields[20].parse().unwrap_or(0.0);
            let bid3_price:   f64  = fields[21].parse().unwrap_or(0.0);

            let ask4_size:    f64  = fields[22].parse().unwrap_or(0.0);
            let ask4_price:   f64  = fields[23].parse().unwrap_or(0.0);
            let bid4_size:    f64  = fields[24].parse().unwrap_or(0.0);
            let bid4_price:   f64  = fields[25].parse().unwrap_or(0.0);

            let ask5_size:    f64  = fields[26].parse().unwrap_or(0.0);
            let ask5_price:   f64  = fields[27].parse().unwrap_or(0.0);
            let bid5_size:    f64  = fields[28].parse().unwrap_or(0.0);
            let bid5_price:   f64  = fields[29].parse().unwrap_or(0.0);

            let ask6_size:    f64  = fields[30].parse().unwrap_or(0.0);
            let ask6_price:   f64  = fields[31].parse().unwrap_or(0.0);
            let bid6_size:    f64  = fields[32].parse().unwrap_or(0.0);
            let bid6_price:   f64  = fields[33].parse().unwrap_or(0.0);

            let ask7_size:    f64  = fields[34].parse().unwrap_or(0.0);
            let ask7_price:   f64  = fields[35].parse().unwrap_or(0.0);
            let bid7_size:    f64  = fields[36].parse().unwrap_or(0.0);
            let bid7_price:   f64  = fields[37].parse().unwrap_or(0.0);

            let ask8_size:    f64  = fields[38].parse().unwrap_or(0.0);
            let ask8_price:   f64  = fields[39].parse().unwrap_or(0.0);
            let bid8_size:    f64  = fields[40].parse().unwrap_or(0.0);
            let bid8_price:   f64  = fields[41].parse().unwrap_or(0.0);

            let ask9_size:    f64  = fields[42].parse().unwrap_or(0.0);
            let ask9_price:   f64  = fields[43].parse().unwrap_or(0.0);
            let bid9_size:    f64  = fields[44].parse().unwrap_or(0.0);
            let bid9_price:   f64  = fields[45].parse().unwrap_or(0.0);

            ids.push(id);
            prices.push(price);
            amounts.push(amount);
            sides.push(side);
            packets.push(packet);
            receveds.push(receved);

            ask0_prices.push(ask0_price);
            ask1_prices.push(ask1_price);
            ask2_prices.push(ask2_price);
            ask3_prices.push(ask3_price);
            ask4_prices.push(ask4_price);
            ask5_prices.push(ask5_price);
            ask6_prices.push(ask6_price);
            ask7_prices.push(ask7_price);
            ask8_prices.push(ask8_price);
            ask9_prices.push(ask9_price);

            bid0_prices.push(bid0_price);
            bid1_prices.push(bid1_price);
            bid2_prices.push(bid2_price);
            bid3_prices.push(bid3_price);
            bid4_prices.push(bid4_price);
            bid5_prices.push(bid5_price);
            bid6_prices.push(bid6_price);
            bid7_prices.push(bid7_price);
            bid8_prices.push(bid8_price);
            bid9_prices.push(bid9_price);

            ask0_sizes.push(ask0_size);
            ask1_sizes.push(ask1_size);
            ask2_sizes.push(ask2_size);
            ask3_sizes.push(ask3_size);
            ask4_sizes.push(ask4_size);
            ask5_sizes.push(ask5_size);
            ask6_sizes.push(ask6_size);
            ask7_sizes.push(ask7_size);
            ask8_sizes.push(ask8_size);
            ask9_sizes.push(ask9_size);

            bid0_sizes.push(bid0_size);
            bid1_sizes.push(bid1_size);
            bid2_sizes.push(bid2_size);
            bid3_sizes.push(bid3_size);
            bid4_sizes.push(bid4_size);
            bid5_sizes.push(bid5_size);
            bid6_sizes.push(bid6_size);
            bid7_sizes.push(bid7_size);
            bid8_sizes.push(bid8_size);
            bid9_sizes.push(bid9_size);

            prev_fields = Some(fields);
        }

        let mut df = DataFrame::new(vec![
            Series::new("id", ids),
            Series::new("price", prices),
            Series::new("amount", amounts),
            Series::new("side", sides),
            Series::new("packet", packets),
            Series::new("receved", receveds),

            Series::new("ask0_price", ask0_prices),
            Series::new("ask1_price", ask1_prices),
            Series::new("ask2_price", ask2_prices),
            Series::new("ask3_price", ask3_prices),
            Series::new("ask4_price", ask4_prices),
            Series::new("ask5_price", ask5_prices),
            Series::new("ask6_price", ask6_prices),
            Series::new("ask7_price", ask7_prices),
            Series::new("ask8_price", ask8_prices),
            Series::new("ask9_price", ask9_prices),

            Series::new("bid0_price", bid0_prices),
            Series::new("bid1_price", bid1_prices),
            Series::new("bid2_price", bid2_prices),
            Series::new("bid3_price", bid3_prices),
            Series::new("bid4_price", bid4_prices),
            Series::new("bid5_price", bid5_prices),
            Series::new("bid6_price", bid6_prices),
            Series::new("bid7_price", bid7_prices),
            Series::new("bid8_price", bid8_prices),
            Series::new("bid9_price", bid9_prices),

            Series::new("ask0_size", ask0_sizes),
            Series::new("ask1_size", ask1_sizes),
            Series::new("ask2_size", ask2_sizes),
            Series::new("ask3_size", ask3_sizes),
            Series::new("ask4_size", ask4_sizes),
            Series::new("ask5_size", ask5_sizes),
            Series::new("ask6_size", ask6_sizes),
            Series::new("ask7_size", ask7_sizes),
            Series::new("ask8_size", ask8_sizes),
            Series::new("ask9_size", ask9_sizes),

            Series::new("bid0_size", bid0_sizes),
            Series::new("bid1_size", bid1_sizes),
            Series::new("bid2_size", bid2_sizes),
            Series::new("bid3_size", bid3_sizes),
            Series::new("bid4_size", bid4_sizes),
            Series::new("bid5_size", bid5_sizes),
            Series::new("bid6_size", bid6_sizes),
            Series::new("bid7_size", bid7_sizes),
            Series::new("bid8_size", bid8_sizes),
            Series::new("bid9_size", bid9_sizes),

            Series::new("order_size", order_sizes),
            Series::new("tot_amt", tot_amts),
        ])?;

        let mut file = File::create(output_file_path)?;
        CsvWriter::new(&mut file).finish(&mut df)?;
    }

    Ok(())
}