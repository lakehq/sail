// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn read_iceberg_rest_catalog() -> Result<(), sail_build_scripts::error::BuildError> {
    let src = "spec/iceberg-rest-catalog.yaml";
    println!("cargo:rerun-if-changed={src}");

    let _openapi = sail_build_scripts::openapi::load_spec(src)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    read_iceberg_rest_catalog()?;
    Ok(())
}
