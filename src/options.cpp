
#include "options.h"

#include "comparator.h"
#include "env.h"

namespace cowbpt {

SliceComparator cmp;

Options::Options() : comparator(&cmp), env(Env::Default()) {}

}
