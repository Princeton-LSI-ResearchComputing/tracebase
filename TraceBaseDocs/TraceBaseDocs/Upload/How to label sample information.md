# How to label sample information

Sample information in TraceBase is kept consistent with existing data in TraceBase.  This document describes in detail where sample information should be stored and how it should be formatted.  It also describes what happens if you have new sample information (ie new Diet, Compound, etc).

If you are unsure of how to label something, write whatever you think fits and upload anyway.  Indicate what you are unsure about in the upload form or on the information sheet.  The developer team will check the data and help you label it properly.

## Animals Sheet

Animal ID
- a unique identifier for the animal.  See recommendations for how to label Animal ID in [Recommended Practices for Organizing Data](Recommended%20Practices%20for%20Organizing%20Data.md).

Age
- age in weeks when the infusion started

Sex
- "male" or "female"

Animal Genotype
- most specific genotype possible.  Check existing data on TraceBase for common genotypes.  If necessary, indicate genotype as "unknown" (e.g. if the animal is a mixed background wildtype).

Animal Body Weight - weight in grams at time of infusion

Infusate
- A description of the infusion solution (aka cocktail) given to this animal.
- Individual tracer compounds will be formatted: **compound_name-[weight element count,weight element count]**
  - Examples:
    - **valine-[13C5,15N1]**
    - **leucine-[13C6,15N1]**
    - **isoleucine-[13C6,15N1]**
- If a tracer is not fully labeled for a given element, the positions of the that element must be included: **compound_name-[position,position-weight element count]**
  - Example:
    - **L-Leucine-[1,2-13C2]**
- Mixtures of compounds are formatted: **tracer_mix_name {tracer; tracer}**
  - **tracer_mix_name** is optional
  - Example:
  - BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}

Tracer Concentrations
- Concentration(s) of infusate in this infusion solution (in mM)
- For multiple tracer compounds, list concentrations in the same order as in "infusate" separated by semicolons
- Example:
  - 5; 6; 9

Infusion Rate
- Volume of infusate solution infused (microliters (ul) per minute per gram of mouse body weight)

Diet
- Description of animal diet used.  Include the manufacturer identification and short description where possible.  Check data on TraceBase for commonly used diets.

Feeding Status
- "fasted", "fed", or "refed"
- Indicate the length of fasting/feeding in "treatment description" or in "study description"

Animal Treatment
- Short, unique identifier for animal treatment protocol.  Details are provided in the "Treatment Description" field on the "Treatments" sheet
- Example:  "T3 in drinking water"
- Default animal treatment is "no treatment"
- Note that unique diets and feeding status are indicated elsewhere, and not considered as "animal treatments"

Study Name
- An identifier for the "experiment" or collection of animals that this animal belongs to.  See [Recommended Practices for Organizing Data](Recommended%20Practices%20for%20Organizing%20Data.md)

Study Description
- A long form description of the study
- Describe here experimental design, citations if the data is published, or any other relevant information that a researcher might need to consider when looking at the data from this study

## Samples Sheet
Sample Name
- Unique identifier for the biological sample
- Must match the name of a sample in Accucor or Isocorr data
- See [Recommended Practices for Organizing Data](Recommended%20Practices%20for%20Organizing%20Data.md) for suggestions on how to name samples

Date collected
- Date sample was collected (YYY-MM-DD)

Researcher Name
- Researcher primarily responsible for collection of this sample
- FIRST LAST

Tissue
- Type of tissue, matching the reference list of tissues
- See "Tissues" tab for reference list of tissues
- If you have a tissue that is not listed in "Tissues" reference, add your new tissue to the "Tissues" tab

Collection Time
- Minutes after the start of the infusion that the tissue was collected
- Collection Time for samples collected before the infusion should be <= 0

Animal ID
- Matches to the Animal ID that this sample was collected from

## Treatments Sheet
Animal Treatment
- Short, unique identifier for animal treatment protocol.  Matches to the same value in "Samples" sheet.

Treatment Description
- A thorough description of an animal treatment protocol. This will be useful for searching and filtering, so use standard terms and be as complete as possible.
- Any difference in Treatment should be indicated by a new Animal Treatment
  - Example:  different doses of T3 in drinking water are indicated by different Animal Treatments

  - | **Animal Treatment**            | **Treatment Description**                                                                                |
    |---------------------------------|----------------------------------------------------------------------------------------------------------|
    | no treatment                    | No treatment was applied to the animal. Animal was housed at room temperature with a normal light cycle. |
    | T3 in drinking water            | T3 was provided in drinking water at 0.5 mg/L for two weeks prior to the start of infusion.              |
    | T3 in drinking water (1.5 mg/L) | T3 was provided in drinking water at 1.5 mg/L for two weeks prior to the start of infusion.              |

## Tissues Sheet
TraceBase Tissue Name
- Short identifier used by TraceBase
- Use the most specific identifier applicable to your samples
- If your data contains a tissue not already listed here, create a new TraceBase Tissue Name and Description here.

Description
- Long form description of TraceBase tissue
